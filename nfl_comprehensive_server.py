# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mcp[cli]>=1.0.0",
# ]
# ///
#!/usr/bin/env python3
"""
NFL Comprehensive MCP Server

A FastMCP server that provides tools to query NFL plays and scores from SQLite databases.

Usage:
    uv run nfl_comprehensive_server.py --team-stats-db nfl_stats.db --plays-db nfl_plays.db --scores-db nfl_scores.db
    
    Or for Claude Desktop, add to your config:
    {
      "mcpServers": {
        "nfl-comprehensive": {
          "command": "uv",
          "args": ["run", "nfl_comprehensive_server.py", 
                   "--team-stats-db", "nfl_stats.db", 
                   "--plays-db", "nfl_plays.db", 
                   "--scores-db", "nfl_scores.db"],
          "cwd": "/path/to/this/directory"
        }
      }
    }
"""

import argparse
import os
import sqlite3
import sys
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

# Global database paths
TEAM_STATS_DB: str = ""
PLAYS_DB: str = ""
SCORES_DB: str = ""

def execute_query(db_path: str, query: str, params=None) -> list[dict[str, Any]]:
    """Execute a query against a specific database and return results."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    except Exception as e:
        raise Exception(f"Database error ({db_path}): {str(e)}")

# Create the MCP server
mcp = FastMCP("NFL Comprehensive Stats")

@mcp.tool()
def get_databases_overview() -> str:
    """Get an overview of all available NFL databases and their contents"""
    response = "NFL Databases Overview:\n\n"
    
    # Team Stats Database
    if TEAM_STATS_DB and os.path.exists(TEAM_STATS_DB):
        try:
            results = execute_query(TEAM_STATS_DB, """
                SELECT season_type, 
                       COUNT(*) as total_records,
                       COUNT(DISTINCT team) as unique_teams,
                       MIN(season) as min_season,
                       MAX(season) as max_season
                FROM team_stats 
                GROUP BY season_type
                ORDER BY season_type
            """)
            
            response += "📊 Team Stats Database:\n"
            for row in results:
                response += f"  {row['season_type']}: {row['total_records']} records, "
                response += f"{row['unique_teams']} teams, seasons {row['min_season']}-{row['max_season']}\n"
        except Exception as e:
            response += f"📊 Team Stats Database: Error - {e}\n"
    else:
        response += "📊 Team Stats Database: Not available\n"
    
    # Plays Database
    if PLAYS_DB and os.path.exists(PLAYS_DB):
        try:
            results = execute_query(PLAYS_DB, """
                SELECT COUNT(*) as total_plays,
                       COUNT(DISTINCT season) as seasons,
                       MIN(season) as min_season,
                       MAX(season) as max_season,
                       COUNT(DISTINCT away_team || '-' || home_team || '-' || date) as unique_games
                FROM plays
            """)
            
            row = results[0]
            response += f"\n🏈 Plays Database:\n"
            response += f"  Total plays: {row['total_plays']:,}\n"
            response += f"  Seasons: {row['seasons']} ({row['min_season']}-{row['max_season']})\n"
            response += f"  Unique games: {row['unique_games']:,}\n"
        except Exception as e:
            response += f"\n🏈 Plays Database: Error - {e}\n"
    else:
        response += "\n🏈 Plays Database: Not available\n"
    
    # Scores Database
    if SCORES_DB and os.path.exists(SCORES_DB):
        try:
            results = execute_query(SCORES_DB, """
                SELECT COUNT(*) as total_games,
                       COUNT(DISTINCT season) as seasons,
                       MIN(season) as min_season,
                       MAX(season) as max_season,
                       COUNT(*) FILTER (WHERE post_season = 1) as playoff_games
                FROM games
            """)
            
            row = results[0]
            response += f"\n🏆 Games Database:\n"
            response += f"  Total games: {row['total_games']:,}\n"
            response += f"  Seasons: {row['seasons']} ({row['min_season']}-{row['max_season']})\n"
            response += f"  Playoff games: {row['playoff_games']:,}\n"
        except Exception as e:
            response += f"\n🏆 Games Database: Error - {e}\n"
    else:
        response += "\n🏆 Games Database: Not available\n"
    
    return response

@mcp.tool()
def get_game_plays(away_team: str, home_team: str, season: int, week: str = None) -> str:
    """Get all plays from a specific game
    
    Args:
        away_team: Away team abbreviation (e.g., 'BUF', 'KC')
        home_team: Home team abbreviation (e.g., 'BUF', 'KC')
        season: Season year
        week: Week number or description (optional, gets first match if not specified)
    """
    away_team = away_team.upper()
    home_team = home_team.upper()
    
    base_query = """
    SELECT season, week, quarter, drive_number, play_number_in_drive,
           team_with_possession, play_outcome, play_description, 
           is_scoring_play, is_scoring_drive
    FROM plays 
    WHERE away_team = ? AND home_team = ? AND season = ?
    """
    
    params = [away_team, home_team, season]
    
    if week:
        base_query += " AND week = ?"
        params.append(week)
    
    base_query += " ORDER BY quarter, drive_number, play_number_in_drive"
    
    try:
        results = execute_query(PLAYS_DB, base_query, tuple(params))
        
        if not results:
            return f"No plays found for {away_team} @ {home_team} in {season}" + (f" week {week}" if week else "")
        
        response = f"Game Plays: {away_team} @ {home_team} - {season}" + (f" Week {week}" if week else "") + f"\n\n"
        response += f"Total plays: {len(results)}\n\n"
        
        current_quarter = None
        current_drive = None
        
        for play in results:
            # Group by quarter and drive
            if play['quarter'] != current_quarter:
                current_quarter = play['quarter']
                response += f"\n=== {current_quarter} ===\n"
                current_drive = None
            
            if play['drive_number'] != current_drive:
                current_drive = play['drive_number']
                response += f"\nDrive {current_drive} ({play['team_with_possession']}):\n"
            
            # Show play details
            scoring_flag = " 🏈" if play['is_scoring_play'] else ""
            response += f"  {play['play_number_in_drive']:2d}. {play['play_outcome']}{scoring_flag}\n"
            if len(results) <= 50:  # Only show descriptions for shorter game logs
                response += f"      {play['play_description'][:100]}{'...' if len(play['play_description']) > 100 else ''}\n"
        
        return response
        
    except Exception as e:
        return f"Error retrieving plays: {str(e)}"

@mcp.tool()
def get_game_score(away_team: str, home_team: str, season: int, week: str = None) -> str:
    """Get the final score and details for a specific game
    
    Args:
        away_team: Away team abbreviation (e.g., 'BUF', 'KC')
        home_team: Home team abbreviation (e.g., 'BUF', 'KC') 
        season: Season year
        week: Week number or description (optional)
    """
    away_team = away_team.upper()
    home_team = home_team.upper()
    
    base_query = """
    SELECT season, week, game_status, day, date,
           away_team, away_record, away_score, away_win,
           home_team, home_record, home_score, home_win,
           away_seeding, home_seeding, post_season
    FROM games 
    WHERE away_team = ? AND home_team = ? AND season = ?
    """
    
    params = [away_team, home_team, season]
    
    if week:
        base_query += " AND week = ?"
        params.append(week)
    
    try:
        results = execute_query(SCORES_DB, base_query, tuple(params))
        
        if not results:
            return f"No game found for {away_team} @ {home_team} in {season}" + (f" week {week}" if week else "")
        
        game = results[0]
        
        response = f"Game Result: {game['away_team']} @ {game['home_team']}\n\n"
        response += f"Date: {game['day']}, {game['date']}\n"
        response += f"Season: {game['season']} Week {game['week']}\n"
        response += f"Status: {game['game_status']}\n"
        
        if game['post_season']:
            response += f"🏆 Playoff Game\n"
            if game['away_seeding']:
                response += f"Seeding: {game['away_team']} #{game['away_seeding']} vs {game['home_team']} #{game['home_seeding']}\n"
        
        response += f"\nScore:\n"
        
        winner = game['away_team'] if game['away_win'] else game['home_team']
        loser = game['home_team'] if game['away_win'] else game['away_team']
        winner_score = game['away_score'] if game['away_win'] else game['home_score']
        loser_score = game['home_score'] if game['away_win'] else game['away_score']
        
        response += f"  {winner} {winner_score:.0f} - {loser} {loser_score:.0f}\n\n"
        
        response += f"Records:\n"
        response += f"  {game['away_team']}: {game['away_record']}\n"
        response += f"  {game['home_team']}: {game['home_record']}\n"
        
        return response
        
    except Exception as e:
        return f"Error retrieving game score: {str(e)}"

@mcp.tool()
def search_plays_by_outcome(play_outcome: str, season: int = None, team: str = None, limit: int = 20) -> str:
    """Search for plays by their outcome (e.g., 'Touchdown', 'Interception', 'Fumble')
    
    Args:
        play_outcome: Type of play outcome to search for
        season: Limit to specific season (optional)
        team: Limit to plays involving specific team (optional)
        limit: Maximum number of results to return (default 20)
    """
    base_query = """
    SELECT season, week, away_team, home_team, quarter,
           team_with_possession, play_outcome, play_description
    FROM plays 
    WHERE play_outcome LIKE ?
    """
    
    params = [f"%{play_outcome}%"]
    
    if season:
        base_query += " AND season = ?"
        params.append(season)
    
    if team:
        team = team.upper()
        base_query += " AND (away_team = ? OR home_team = ? OR team_with_possession = ?)"
        params.extend([team, team, team])
    
    base_query += " ORDER BY season DESC, week DESC LIMIT ?"
    params.append(limit)
    
    try:
        results = execute_query(PLAYS_DB, base_query, tuple(params))
        
        if not results:
            search_desc = f"'{play_outcome}'"
            if season:
                search_desc += f" in {season}"
            if team:
                search_desc += f" involving {team}"
            return f"No plays found for {search_desc}"
        
        response = f"Plays with outcome '{play_outcome}'"
        if season:
            response += f" ({season})"
        if team:
            response += f" involving {team}"
        response += f":\n\n"
        
        for i, play in enumerate(results, 1):
            response += f"{i:2d}. {play['season']} Week {play['week']} - {play['away_team']} @ {play['home_team']}\n"
            response += f"    {play['quarter']} - {play['team_with_possession']}: {play['play_outcome']}\n"
            response += f"    {play['play_description'][:100]}{'...' if len(play['play_description']) > 100 else ''}\n\n"
        
        return response
        
    except Exception as e:
        return f"Error searching plays: {str(e)}"

@mcp.tool()
def get_team_season_record(team: str, season: int) -> str:
    """Get a team's complete season record including all games
    
    Args:
        team: Team abbreviation (e.g., 'BUF', 'KC', 'GB')
        season: Season year
    """
    team = team.upper()
    
    query = """
    SELECT week, day, date, game_status,
           away_team, away_score, home_team, home_score,
           CASE 
               WHEN away_team = ? THEN away_win
               ELSE home_win
           END as team_won,
           CASE 
               WHEN away_team = ? THEN away_score
               ELSE home_score  
           END as team_score,
           CASE 
               WHEN away_team = ? THEN home_score
               ELSE away_score
           END as opponent_score,
           CASE 
               WHEN away_team = ? THEN home_team
               ELSE away_team
           END as opponent,
           CASE 
               WHEN away_team = ? THEN 'Away'
               ELSE 'Home'
           END as home_away,
           post_season
    FROM games 
    WHERE (away_team = ? OR home_team = ?) AND season = ?
    ORDER BY 
        post_season ASC,
        CASE 
            WHEN week LIKE '%Preseason%' THEN 0
            WHEN week = 'Wild Card' THEN 19
            WHEN week = 'Divisional' THEN 20
            WHEN week = 'Conference' THEN 21
            WHEN week = 'Super Bowl' THEN 22
            ELSE CAST(week AS INTEGER)
        END
    """
    
    try:
        results = execute_query(SCORES_DB, query, (team, team, team, team, team, team, team, season))
        
        if not results:
            return f"No games found for {team} in {season}"
        
        # Calculate record
        regular_wins = sum(1 for game in results if game['team_won'] and not game['post_season'])
        regular_losses = sum(1 for game in results if not game['team_won'] and not game['post_season'])
        playoff_wins = sum(1 for game in results if game['team_won'] and game['post_season'])
        playoff_losses = sum(1 for game in results if not game['team_won'] and game['post_season'])
        
        response = f"{team} {season} Season Record\n\n"
        response += f"Regular Season: {regular_wins}-{regular_losses}\n"
        if playoff_wins or playoff_losses:
            response += f"Playoffs: {playoff_wins}-{playoff_losses}\n"
        response += f"\nGames:\n"
        
        current_season_type = None
        for game in results:
            # Separate regular season and playoffs
            if game['post_season'] and current_season_type != 'playoffs':
                current_season_type = 'playoffs'
                response += f"\nPlayoffs:\n"
            elif not game['post_season'] and current_season_type != 'regular':
                current_season_type = 'regular'
                if current_season_type != None:  # Don't add header for first section
                    response += f"\nRegular Season:\n"
            
            result = "W" if game['team_won'] else "L"
            location = "vs" if game['home_away'] == 'Home' else "@"
            
            response += f"  Week {game['week']:<12} {result} {location} {game['opponent']:<3} "
            response += f"{game['team_score']:.0f}-{game['opponent_score']:.0f}\n"
        
        return response
        
    except Exception as e:
        return f"Error retrieving season record: {str(e)}"

@mcp.tool()
def get_playoff_results(season: int, round_name: str = None) -> str:
    """Get playoff results for a specific season
    
    Args:
        season: Season year
        round_name: Specific playoff round (Wild Card, Divisional, Conference, Super Bowl) - optional
    """
    base_query = """
    SELECT week, day, date, away_team, away_score, away_seeding,
           home_team, home_score, home_seeding, away_win, home_win
    FROM games 
    WHERE season = ? AND post_season = 1
    """
    
    params = [season]
    
    if round_name:
        base_query += " AND week = ?"
        params.append(round_name)
    
    base_query += " ORDER BY CASE week WHEN 'Wild Card' THEN 1 WHEN 'Divisional' THEN 2 WHEN 'Conference' THEN 3 WHEN 'Super Bowl' THEN 4 END, date"
    
    try:
        results = execute_query(SCORES_DB, base_query, tuple(params))
        
        if not results:
            search_desc = f"{season} playoffs"
            if round_name:
                search_desc = f"{season} {round_name}"
            return f"No playoff games found for {search_desc}"
        
        response = f"🏆 {season} NFL Playoffs"
        if round_name:
            response += f" - {round_name}"
        response += f"\n\n"
        
        current_round = None
        for game in results:
            if game['week'] != current_round:
                current_round = game['week']
                response += f"\n{current_round}:\n"
            
            winner = game['away_team'] if game['away_win'] else game['home_team']
            loser = game['home_team'] if game['away_win'] else game['away_team']
            winner_score = game['away_score'] if game['away_win'] else game['home_score']
            loser_score = game['home_score'] if game['away_win'] else game['away_score']
            winner_seed = game['away_seeding'] if game['away_win'] else game['home_seeding']
            loser_seed = game['home_seeding'] if game['away_win'] else game['away_seeding']
            
            seed_display = ""
            if winner_seed and loser_seed:
                seed_display = f" (#{winner_seed} def #{loser_seed})"
            
            response += f"  {winner} {winner_score:.0f} - {loser} {loser_score:.0f}{seed_display}\n"
        
        return response
        
    except Exception as e:
        return f"Error retrieving playoff results: {str(e)}"

# Include the original team stats tools from the previous server
@mcp.tool()
def get_team_stats(team: str, season: int | None = None, season_type: str = "REG") -> str:
    """Get detailed statistics for a specific NFL team from team stats database
    
    Args:
        team: Team abbreviation (e.g., 'BUF', 'KC', 'GB')
        season: Season year (optional, gets most recent if not specified)
        season_type: Season type - 'REG' for regular season, 'POST' for postseason
    """
    if not TEAM_STATS_DB or not os.path.exists(TEAM_STATS_DB):
        return "Team stats database not available"
    
    team = team.upper()
    
    if season:
        query = """
        SELECT * FROM team_stats 
        WHERE team = ? AND season = ? AND season_type = ?
        """
        results = execute_query(TEAM_STATS_DB, query, (team, season, season_type))
    else:
        query = """
        SELECT * FROM team_stats 
        WHERE team = ? AND season_type = ?
        ORDER BY season DESC LIMIT 3
        """
        results = execute_query(TEAM_STATS_DB, query, (team, season_type))
    
    if not results:
        return f"No team stats found for {team}" + (f" in {season}" if season else "") + f" ({season_type})"
    
    response = f"Team Stats for {team}" + (f" - {season}" if season else "") + f" ({season_type}):\n\n"
    
    for row in results:
        response += f"Season: {row['season']}\n"
        response += f"Games: {row['games']}\n"
        response += f"Passing Yards: {row['passing_yards']}\n"
        response += f"Passing TDs: {row['passing_tds']}\n"
        response += f"Rushing Yards: {row['rushing_yards']}\n"
        response += f"Defensive Sacks: {row['def_sacks']}\n"
        response += "\n"
    
    return response

def main():
    global TEAM_STATS_DB, PLAYS_DB, SCORES_DB
    
    # Check for environment variables first (for mcp dev compatibility)
    TEAM_STATS_DB = os.environ.get('TEAM_STATS_DB', '')
    PLAYS_DB = os.environ.get('PLAYS_DB', '')
    SCORES_DB = os.environ.get('SCORES_DB', '')
    
    if not (TEAM_STATS_DB and PLAYS_DB and SCORES_DB):
        parser = argparse.ArgumentParser(description="NFL Comprehensive MCP Server")
        parser.add_argument("--team-stats-db", help="Path to team stats SQLite database file")
        parser.add_argument("--plays-db", help="Path to plays SQLite database file") 
        parser.add_argument("--scores-db", help="Path to scores SQLite database file")
        args = parser.parse_args()
        
        TEAM_STATS_DB = args.team_stats_db or TEAM_STATS_DB
        PLAYS_DB = args.plays_db or PLAYS_DB
        SCORES_DB = args.scores_db or SCORES_DB
    
    # Verify at least one database exists
    available_dbs = []
    if TEAM_STATS_DB and os.path.exists(TEAM_STATS_DB):
        available_dbs.append("team stats")
    if PLAYS_DB and os.path.exists(PLAYS_DB):
        available_dbs.append("plays")
    if SCORES_DB and os.path.exists(SCORES_DB):
        available_dbs.append("scores")
    
    if not available_dbs:
        print("Error: No valid databases found", file=sys.stderr)
        print(f"Checked paths:", file=sys.stderr)
        print(f"  Team stats: {TEAM_STATS_DB}", file=sys.stderr)
        print(f"  Plays: {PLAYS_DB}", file=sys.stderr)
        print(f"  Scores: {SCORES_DB}", file=sys.stderr)
        sys.exit(1)
    
    print(f"NFL Comprehensive Server - Connected to: {', '.join(available_dbs)} database(s)", file=sys.stderr)
    
    # Run the server
    mcp.run()

if __name__ == "__main__":
    main()