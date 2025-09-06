# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pandas>=2.0",
# ]
# ///
#!/usr/bin/env python3
"""
NFL CSV Data Importer

Imports all CSV files from the nfl_stats directory into two SQLite databases:
- nfl_plays.db: Contains all play-by-play data
- nfl_scores.db: Contains all game scores and results

Usage:
    uv run nfl_csv_importer.py --stats-dir nfl_stats --plays-db nfl_plays.db --scores-db nfl_scores.db
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd


class NFLDataImporter:
    def __init__(self, stats_dir: str, plays_db: str, scores_db: str):
        self.stats_dir = Path(stats_dir)
        self.plays_db = plays_db
        self.scores_db = scores_db
        
        # Expected column schemas
        self.plays_columns = [
            "Season", "Week", "Day", "Date", "AwayTeam", "HomeTeam", 
            "Quarter", "DriveNumber", "TeamWithPossession", "IsScoringDrive",
            "PlayNumberInDrive", "IsScoringPlay", "PlayOutcome", 
            "PlayDescription", "PlayStart"
        ]
        
        self.scores_columns = [
            "Season", "Week", "GameStatus", "Day", "Date", "AwayTeam",
            "AwayRecord", "AwayScore", "AwayWin", "HomeTeam", "HomeRecord",
            "HomeScore", "HomeWin", "AwaySeeding", "HomeSeeding", "PostSeason"
        ]
    
    def setup_databases(self):
        """Create database tables with proper schemas."""
        print("Setting up databases...")
        
        # Setup plays database
        conn_plays = sqlite3.connect(self.plays_db)
        conn_plays.execute("""
            CREATE TABLE IF NOT EXISTS plays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                season INTEGER,
                week TEXT,
                day TEXT,
                date TEXT,
                away_team TEXT,
                home_team TEXT,
                quarter TEXT,
                drive_number INTEGER,
                team_with_possession TEXT,
                is_scoring_drive INTEGER,
                play_number_in_drive INTEGER,
                is_scoring_play INTEGER,
                play_outcome TEXT,
                play_description TEXT,
                play_start TEXT,
                UNIQUE(season, week, away_team, home_team, quarter, drive_number, play_number_in_drive)
            );
        """)
        
        # Create indexes for common queries
        conn_plays.execute("CREATE INDEX IF NOT EXISTS idx_plays_season ON plays(season);")
        conn_plays.execute("CREATE INDEX IF NOT EXISTS idx_plays_teams ON plays(away_team, home_team);")
        conn_plays.execute("CREATE INDEX IF NOT EXISTS idx_plays_possession ON plays(team_with_possession);")
        conn_plays.execute("CREATE INDEX IF NOT EXISTS idx_plays_scoring ON plays(is_scoring_play);")
        
        conn_plays.commit()
        conn_plays.close()
        
        # Setup scores database
        conn_scores = sqlite3.connect(self.scores_db)
        conn_scores.execute("""
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                season INTEGER,
                week TEXT,
                game_status TEXT,
                day TEXT,
                date TEXT,
                away_team TEXT,
                away_record TEXT,
                away_score REAL,
                away_win REAL,
                home_team TEXT,
                home_record TEXT,
                home_score REAL,
                home_win REAL,
                away_seeding TEXT,
                home_seeding TEXT,
                post_season INTEGER,
                UNIQUE(season, week, away_team, home_team, date)
            );
        """)
        
        # Create indexes for common queries
        conn_scores.execute("CREATE INDEX IF NOT EXISTS idx_games_season ON games(season);")
        conn_scores.execute("CREATE INDEX IF NOT EXISTS idx_games_teams ON games(away_team, home_team);")
        conn_scores.execute("CREATE INDEX IF NOT EXISTS idx_games_postseason ON games(post_season);")
        conn_scores.execute("CREATE INDEX IF NOT EXISTS idx_games_date ON games(date);")
        
        conn_scores.commit()
        conn_scores.close()
        
        print(f"✅ Created databases: {self.plays_db}, {self.scores_db}")
    
    def get_csv_files(self) -> Dict[str, List[Path]]:
        """Get all CSV files categorized by type."""
        if not self.stats_dir.exists():
            raise FileNotFoundError(f"Stats directory not found: {self.stats_dir}")
        
        plays_files = []
        scores_files = []
        
        for file_path in self.stats_dir.glob("*.csv"):
            if "plays" in file_path.name.lower():
                plays_files.append(file_path)
            elif "scores" in file_path.name.lower():
                scores_files.append(file_path)
            else:
                print(f"⚠️  Unknown CSV file type: {file_path.name}")
        
        plays_files.sort()
        scores_files.sort()
        
        return {"plays": plays_files, "scores": scores_files}
    
    def import_plays_file(self, file_path: Path) -> int:
        """Import a single plays CSV file."""
        print(f"📊 Importing plays: {file_path.name}")
        
        try:
            # Read CSV with pandas
            df = pd.read_csv(file_path, low_memory=False)
            
            # Normalize column names to match database schema
            column_mapping = {
                "Season": "season",
                "Week": "week", 
                "Day": "day",
                "Date": "date",
                "AwayTeam": "away_team",
                "HomeTeam": "home_team",
                "Quarter": "quarter",
                "DriveNumber": "drive_number",
                "TeamWithPossession": "team_with_possession",
                "IsScoringDrive": "is_scoring_drive",
                "PlayNumberInDrive": "play_number_in_drive",
                "IsScoringPlay": "is_scoring_play",
                "PlayOutcome": "play_outcome",
                "PlayDescription": "play_description",
                "PlayStart": "play_start"
            }
            
            df = df.rename(columns=column_mapping)
            
            # Connect to database and insert data
            conn = sqlite3.connect(self.plays_db)
            
            # Insert data in chunks to handle large files
            chunk_size = 10000
            rows_inserted = 0
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                try:
                    chunk.to_sql("plays", conn, if_exists="append", index=False)
                    rows_inserted += len(chunk)
                    if i % (chunk_size * 10) == 0:  # Progress every 100k rows
                        print(f"   Inserted {rows_inserted:,} rows...")
                except sqlite3.IntegrityError as e:
                    if "UNIQUE constraint failed" in str(e):
                        # Handle duplicate rows by inserting one by one
                        for _, row in chunk.iterrows():
                            try:
                                pd.DataFrame([row]).to_sql("plays", conn, if_exists="append", index=False)
                                rows_inserted += 1
                            except sqlite3.IntegrityError:
                                pass  # Skip duplicates
                    else:
                        raise
            
            conn.commit()
            conn.close()
            
            print(f"   ✅ Inserted {rows_inserted:,} plays from {file_path.name}")
            return rows_inserted
            
        except Exception as e:
            print(f"   ❌ Error importing {file_path.name}: {e}")
            return 0
    
    def import_scores_file(self, file_path: Path) -> int:
        """Import a single scores CSV file."""
        print(f"🏈 Importing scores: {file_path.name}")
        
        try:
            # Read CSV with pandas
            df = pd.read_csv(file_path, low_memory=False)
            
            # Normalize column names to match database schema
            column_mapping = {
                "Season": "season",
                "Week": "week",
                "GameStatus": "game_status",
                "Day": "day",
                "Date": "date",
                "AwayTeam": "away_team",
                "AwayRecord": "away_record",
                "AwayScore": "away_score",
                "AwayWin": "away_win",
                "HomeTeam": "home_team",
                "HomeRecord": "home_record",
                "HomeScore": "home_score",
                "HomeWin": "home_win",
                "AwaySeeding": "away_seeding",
                "HomeSeeding": "home_seeding",
                "PostSeason": "post_season"
            }
            
            df = df.rename(columns=column_mapping)
            
            # Connect to database and insert data
            conn = sqlite3.connect(self.scores_db)
            
            rows_inserted = 0
            try:
                df.to_sql("games", conn, if_exists="append", index=False)
                rows_inserted = len(df)
            except sqlite3.IntegrityError:
                # Handle duplicates by inserting one by one
                for _, row in df.iterrows():
                    try:
                        pd.DataFrame([row]).to_sql("games", conn, if_exists="append", index=False)
                        rows_inserted += 1
                    except sqlite3.IntegrityError:
                        pass  # Skip duplicates
            
            conn.commit()
            conn.close()
            
            print(f"   ✅ Inserted {rows_inserted:,} games from {file_path.name}")
            return rows_inserted
            
        except Exception as e:
            print(f"   ❌ Error importing {file_path.name}: {e}")
            return 0
    
    def import_all_data(self):
        """Import all CSV files into their respective databases."""
        csv_files = self.get_csv_files()
        
        print(f"\n🔍 Found files:")
        print(f"   Plays files: {len(csv_files['plays'])}")
        print(f"   Scores files: {len(csv_files['scores'])}")
        
        # Import plays data
        total_plays = 0
        if csv_files['plays']:
            print(f"\n📊 Importing plays data...")
            for file_path in csv_files['plays']:
                total_plays += self.import_plays_file(file_path)
        
        # Import scores data  
        total_games = 0
        if csv_files['scores']:
            print(f"\n🏈 Importing scores data...")
            for file_path in csv_files['scores']:
                total_games += self.import_scores_file(file_path)
        
        # Summary
        print(f"\n🎉 Import complete!")
        print(f"   Total plays imported: {total_plays:,}")
        print(f"   Total games imported: {total_games:,}")
        print(f"   Databases created:")
        print(f"     • {self.plays_db}")
        print(f"     • {self.scores_db}")
    
    def show_summary(self):
        """Show summary statistics of imported data."""
        print(f"\n📈 Database Summary:")
        
        # Plays database summary
        if os.path.exists(self.plays_db):
            conn = sqlite3.connect(self.plays_db)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM plays")
            total_plays = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT season) FROM plays")
            seasons = cursor.fetchone()[0]
            
            cursor.execute("SELECT MIN(season), MAX(season) FROM plays")
            season_range = cursor.fetchone()
            
            cursor.execute("SELECT COUNT(DISTINCT away_team || home_team) FROM plays")
            unique_games = cursor.fetchone()[0]
            
            print(f"\n🎯 Plays Database ({self.plays_db}):")
            print(f"   Total plays: {total_plays:,}")
            print(f"   Seasons: {seasons} ({season_range[0]}-{season_range[1]})")
            print(f"   Unique games: {unique_games:,}")
            
            conn.close()
        
        # Scores database summary
        if os.path.exists(self.scores_db):
            conn = sqlite3.connect(self.scores_db)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM games")
            total_games = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT season) FROM games")
            seasons = cursor.fetchone()[0]
            
            cursor.execute("SELECT MIN(season), MAX(season) FROM games")
            season_range = cursor.fetchone()
            
            cursor.execute("SELECT COUNT(*) FROM games WHERE post_season = 1")
            playoff_games = cursor.fetchone()[0]
            
            print(f"\n🏆 Games Database ({self.scores_db}):")
            print(f"   Total games: {total_games:,}")
            print(f"   Seasons: {seasons} ({season_range[0]}-{season_range[1]})")
            print(f"   Playoff games: {playoff_games:,}")
            
            conn.close()


def main():
    parser = argparse.ArgumentParser(description="Import NFL CSV data into SQLite databases")
    parser.add_argument("--stats-dir", default="nfl_stats", help="Directory containing CSV files")
    parser.add_argument("--plays-db", default="nfl_plays.db", help="SQLite database for plays data")
    parser.add_argument("--scores-db", default="nfl_scores.db", help="SQLite database for scores data")
    parser.add_argument("--summary-only", action="store_true", help="Show summary of existing databases")
    
    args = parser.parse_args()
    
    importer = NFLDataImporter(args.stats_dir, args.plays_db, args.scores_db)
    
    if args.summary_only:
        importer.show_summary()
    else:
        try:
            importer.setup_databases()
            importer.import_all_data()
            importer.show_summary()
        except Exception as e:
            print(f"❌ Import failed: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()