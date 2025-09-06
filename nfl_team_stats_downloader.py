# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pandas>=2.0",
#   "requests>=2.31",
# ]
# ///
#!/usr/bin/env python3
"""
NFL Team Stats Downloader

Downloads team stats CSV files from nflverse-data GitHub releases and stores them
in SQLite database with separate handling for regular season and postseason data.

Usage with uv (recommended):
  uv run nfl_team_stats_downloader.py --db nfl_stats.db --years 2023 2024

Usage with regular Python:
  python nfl_team_stats_downloader.py --db nfl_stats.db --years 2023 2024
"""

import argparse
import hashlib
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pandas as pd
import requests

OWNER = "nflverse"
REPO = "nflverse-data"
API_BASE = f"https://api.github.com/repos/{OWNER}/{REPO}"

# Expected columns for team stats
TEAM_STATS_COLUMNS = [
    "season", "team", "season_type", "games", "completions", "attempts", "passing_yards", 
    "passing_tds", "passing_interceptions", "sacks_suffered", "sack_yards_lost", "sack_fumbles", 
    "sack_fumbles_lost", "passing_air_yards", "passing_yards_after_catch", "passing_first_downs", 
    "passing_epa", "passing_cpoe", "passing_2pt_conversions", "carries", "rushing_yards", 
    "rushing_tds", "rushing_fumbles", "rushing_fumbles_lost", "rushing_first_downs", "rushing_epa", 
    "rushing_2pt_conversions", "receptions", "targets", "receiving_yards", "receiving_tds", 
    "receiving_fumbles", "receiving_fumbles_lost", "receiving_air_yards", "receiving_yards_after_catch", 
    "receiving_first_downs", "receiving_epa", "receiving_2pt_conversions", "special_teams_tds", 
    "def_tackles_solo", "def_tackles_with_assist", "def_tackle_assists", "def_tackles_for_loss", 
    "def_tackles_for_loss_yards", "def_fumbles_forced", "def_sacks", "def_sack_yards", "def_qb_hits", 
    "def_interceptions", "def_interception_yards", "def_pass_defended", "def_tds", "def_fumbles", 
    "def_safeties", "misc_yards", "fumble_recovery_own", "fumble_recovery_yards_own", 
    "fumble_recovery_opp", "fumble_recovery_yards_opp", "fumble_recovery_tds", "penalties", 
    "penalty_yards", "timeouts", "punt_returns", "punt_return_yards", "kickoff_returns", 
    "kickoff_return_yards", "fg_made", "fg_att", "fg_missed", "fg_blocked", "fg_long", "fg_pct", 
    "fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49", "fg_made_50_59", "fg_made_60_", 
    "fg_missed_0_19", "fg_missed_20_29", "fg_missed_30_39", "fg_missed_40_49", "fg_missed_50_59", 
    "fg_missed_60_", "fg_made_list", "fg_missed_list", "fg_blocked_list", "fg_made_distance", 
    "fg_missed_distance", "fg_blocked_distance", "pat_made", "pat_att", "pat_missed", "pat_blocked", 
    "pat_pct", "gwfg_made", "gwfg_att", "gwfg_missed", "gwfg_blocked", "gwfg_distance_list"
]

def setup_database(conn: sqlite3.Connection) -> None:
    """Create tables for team stats and download log."""
    
    # Create team stats table with season_type flag
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS team_stats (
            season INTEGER,
            team TEXT,
            season_type TEXT,  -- 'REG' or 'POST'
            {', '.join(f'{col} TEXT' for col in TEAM_STATS_COLUMNS[3:])},  -- Skip season, team, season_type
            PRIMARY KEY (season, team, season_type)
        );
    """)
    
    # Create download log table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS download_log (
            asset_id INTEGER,
            asset_name TEXT,
            file_type TEXT,  -- 'regular' or 'postseason'
            year INTEGER,
            sha256 TEXT,
            rows_ingested INTEGER,
            status TEXT,
            downloaded_at TEXT,
            PRIMARY KEY (asset_id, file_type)
        );
    """)
    
    conn.commit()

def get_github_releases(session: requests.Session, tag: str = "stats_team") -> List[dict]:
    """Get GitHub releases for the specified tag."""
    url = f"{API_BASE}/releases/tags/{tag}"
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return [response.json()]

def filter_team_stats_assets(assets: List[dict], years: Optional[List[int]] = None) -> Tuple[List[dict], List[dict]]:
    """Filter assets to get regular season and postseason team stats files."""
    reg_assets = []
    post_assets = []
    
    for asset in assets:
        name = asset.get("name", "")
        
        # Extract year from filename
        year_match = re.search(r'(\d{4})', name)
        if not year_match:
            continue
            
        year = int(year_match.group(1))
        if years and year not in years:
            continue
            
        if name.startswith("stats_team_reg_") and name.endswith(".csv"):
            reg_assets.append(asset)
        elif name.startswith("stats_team_post_") and name.endswith(".csv"):
            post_assets.append(asset)
    
    return reg_assets, post_assets

def download_and_process_csv(session: requests.Session, asset: dict, 
                           conn: sqlite3.Connection, season_type: str) -> int:
    """Download and process a team stats CSV file."""
    asset_id = asset.get("id")
    asset_name = asset.get("name", "")
    download_url = asset.get("browser_download_url")
    
    # Extract year from filename
    year_match = re.search(r'(\d{4})', asset_name)
    if not year_match:
        return 0
        
    year = int(year_match.group(1))
    
    # Check if already processed
    cursor = conn.execute(
        "SELECT 1 FROM download_log WHERE asset_id = ? AND file_type = ?",
        (asset_id, season_type.lower())
    )
    if cursor.fetchone():
        print(f"[skip] {asset_name} already processed")
        return 0
    
    print(f"[download] {asset_name}")
    
    # Download CSV
    response = session.get(download_url, timeout=120)
    response.raise_for_status()
    
    # Calculate SHA256
    sha256 = hashlib.sha256(response.content).hexdigest()
    
    # Read CSV into DataFrame
    df = pd.read_csv(pd.io.common.StringIO(response.text))
    
    # Add season_type column
    df['season_type'] = season_type
    
    # Ensure all expected columns exist
    for col in TEAM_STATS_COLUMNS:
        if col not in df.columns:
            df[col] = None
    
    # Select only the expected columns in the correct order
    df = df[TEAM_STATS_COLUMNS]
    
    # Insert data
    rows_inserted = 0
    print(f"[processing] {asset_name}: Processing {len(df)} rows")
    print(f"[columns] Available columns: {list(df.columns)}")
    print(f"[sample] First row data:")
    if not df.empty:
        first_row = df.iloc[0]
        for col in ['season', 'team', 'season_type', 'games', 'passing_yards', 'rushing_yards']:
            if col in first_row:
                print(f"  {col}: {first_row[col]}")
    
    for idx, row in df.iterrows():
        try:
            # Log every 10th row for debugging
            if rows_inserted % 10 == 0 or rows_inserted < 5:
                print(f"[insert] Row {rows_inserted + 1}: {row['team']} {row['season']} {row['season_type']}")
            
            conn.execute(f"""
                INSERT OR REPLACE INTO team_stats 
                ({', '.join(TEAM_STATS_COLUMNS)})
                VALUES ({', '.join(['?' for _ in TEAM_STATS_COLUMNS])})
            """, tuple(row))
            rows_inserted += 1
        except Exception as e:
            print(f"Error inserting row {rows_inserted + 1}: {e}")
            print(f"  Row data: team={row.get('team', 'N/A')}, season={row.get('season', 'N/A')}, season_type={row.get('season_type', 'N/A')}")
            continue
    
    # Log the download
    conn.execute("""
        INSERT OR REPLACE INTO download_log 
        (asset_id, asset_name, file_type, year, sha256, rows_ingested, status, downloaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (asset_id, asset_name, season_type.lower(), year, sha256, rows_inserted, 
          "success", datetime.now(timezone.utc).isoformat()))
    
    conn.commit()
    print(f"[success] {asset_name}: {rows_inserted} rows inserted")
    
    return rows_inserted

def main():
    parser = argparse.ArgumentParser(description="Download NFL team stats CSV files")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--years", nargs="*", type=int, help="Years to download (default: all)")
    parser.add_argument("--github-token", help="GitHub API token")
    
    args = parser.parse_args()
    
    # Setup session
    session = requests.Session()
    session.headers.update({
        "User-Agent": "nfl-team-stats-downloader/1.0",
        "Accept": "application/vnd.github+json"
    })
    
    if args.github_token:
        session.headers["Authorization"] = f"Bearer {args.github_token}"
    
    # Setup database
    conn = sqlite3.connect(args.db)
    setup_database(conn)
    
    try:
        # Get releases
        releases = get_github_releases(session, "stats_team")
        
        total_rows = 0
        
        for release in releases:
            assets = release.get("assets", [])
            reg_assets, post_assets = filter_team_stats_assets(assets, args.years)
            
            print(f"\nFound {len(reg_assets)} regular season files and {len(post_assets)} postseason files")
            
            # Process regular season files
            for asset in reg_assets:
                rows = download_and_process_csv(session, asset, conn, "REG")
                total_rows += rows
            
            # Process postseason files
            for asset in post_assets:
                rows = download_and_process_csv(session, asset, conn, "POST")
                total_rows += rows
        
        print(f"\nTotal rows inserted: {total_rows}")
        
        # Show detailed summary
        cursor = conn.execute("""
            SELECT season_type, season, COUNT(*) as teams
            FROM team_stats 
            GROUP BY season_type, season
            ORDER BY season, season_type
        """)
        
        print("\nDetailed database summary:")
        for row in cursor:
            print(f"  {row[1]} {row[0]}: {row[2]} teams")
            
        # Show overall summary
        cursor = conn.execute("""
            SELECT season_type, COUNT(*) as teams, 
                   COUNT(DISTINCT season) as seasons,
                   MIN(season) as min_season,
                   MAX(season) as max_season
            FROM team_stats 
            GROUP BY season_type
        """)
        
        print("\nOverall summary:")
        for row in cursor:
            print(f"  {row[0]}: {row[1]} team records across {row[2]} seasons ({row[3]}-{row[4]})")
            
        # Show sample data
        cursor = conn.execute("""
            SELECT season, team, season_type, games, passing_yards, rushing_yards
            FROM team_stats 
            ORDER BY season DESC, team
            LIMIT 5
        """)
        
        print("\nSample data (first 5 rows):")
        for row in cursor:
            print(f"  {row[0]} {row[1]} ({row[2]}): {row[3]} games, {row[4]} pass yds, {row[5]} rush yds")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()