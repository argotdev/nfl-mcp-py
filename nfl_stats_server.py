# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mcp>=1.0.0",
# ]
# ///
#!/usr/bin/env python3
"""
NFL Team Stats MCP Server

A FastMCP server that provides tools to query NFL team statistics from SQLite database.

Usage:
    uv run nfl_stats_server.py --db nfl_stats.db
    
    Or for Claude Desktop, add to your config:
    {
      "mcpServers": {
        "nfl-stats": {
          "command": "uv",
          "args": ["run", "nfl_stats_server.py", "--db", "nfl_stats.db"],
          "cwd": "/path/to/this/directory"
        }
      }
    }
"""

import argparse
import os
import sqlite3
import sys
from typing import Any

from mcp.server.fastmcp import FastMCP

# Global database path
DB_PATH: str = ""

def execute_query(query: str, params=None) -> list[dict[str, Any]]:
    """Execute a query and return results as list of dictionaries."""
    try:
        conn = sqlite3.connect(DB_PATH)
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
        raise Exception(f"Database error: {str(e)}")

# Create the MCP server
mcp = FastMCP("NFL Team Stats")

@mcp.tool()
def get_data_overview() -> str:
    """Get an overview of what NFL team stats data is available in the database"""
    query = """
    SELECT season_type, 
           COUNT(*) as total_records,
           COUNT(DISTINCT team) as unique_teams,
           COUNT(DISTINCT season) as seasons_covered,
           MIN(season) as earliest_season,
           MAX(season) as latest_season
    FROM team_stats 
    GROUP BY season_type
    ORDER BY season_type
    """
    results = execute_query(query)
    
    response = "NFL Team Stats Database Overview:\n\n"
    for row in results:
        response += f"Season Type: {row['season_type']}\n"
        response += f"  - Total Records: {row['total_records']}\n"
        response += f"  - Unique Teams: {row['unique_teams']}\n"
        response += f"  - Seasons Covered: {row['seasons_covered']} ({row['earliest_season']}-{row['latest_season']})\n\n"
    
    return response

@mcp.tool()
def get_team_stats(team: str, season: int | None = None, season_type: str = "REG") -> str:
    """Get detailed statistics for a specific NFL team
    
    Args:
        team: Team abbreviation (e.g., 'BUF', 'KC', 'GB')
        season: Season year (optional, gets most recent if not specified)
        season_type: Season type - 'REG' for regular season, 'POST' for postseason
    """
    team = team.upper()
    
    if season:
        query = """
        SELECT * FROM team_stats 
        WHERE team = ? AND season = ? AND season_type = ?
        """
        results = execute_query(query, (team, season, season_type))
    else:
        query = """
        SELECT * FROM team_stats 
        WHERE team = ? AND season_type = ?
        ORDER BY season DESC LIMIT 3
        """
        results = execute_query(query, (team, season_type))
    
    if not results:
        return f"No data found for team {team}" + (f" in {season}" if season else "") + f" ({season_type})"
    
    response = f"Stats for {team}" + (f" - {season}" if season else "") + f" ({season_type}):\n\n"
    
    for row in results:
        response += f"Season: {row['season']}\n"
        response += f"Games: {row['games']}\n"
        response += f"Passing Yards: {row['passing_yards']}\n"
        response += f"Passing TDs: {row['passing_tds']}\n"
        response += f"Passing INTs: {row['passing_interceptions']}\n"
        response += f"Rushing Yards: {row['rushing_yards']}\n"
        response += f"Rushing TDs: {row['rushing_tds']}\n"
        response += f"Receiving Yards: {row['receiving_yards']}\n"
        response += f"Receiving TDs: {row['receiving_tds']}\n"
        response += f"Defensive Sacks: {row['def_sacks']}\n"
        response += f"Defensive INTs: {row['def_interceptions']}\n"
        if row.get('fg_pct'):
            response += f"FG Percentage: {row['fg_pct']}%\n"
        response += "\n"
    
    return response

@mcp.tool()
def get_stat_leaders(stat_column: str, season: int | None = None, season_type: str = "REG", limit: int = 10) -> str:
    """Get the top performers in a specific statistical category
    
    Args:
        stat_column: Statistical category (e.g., 'passing_yards', 'rushing_yards', 'def_sacks')
        season: Season year (optional, gets all seasons if not specified)
        season_type: Season type - 'REG' for regular season, 'POST' for postseason
        limit: Number of top results to return (default 10)
    """
    base_query = f"""
    SELECT season, team, season_type, games,
           CAST({stat_column} AS INTEGER) as {stat_column}
    FROM team_stats 
    WHERE season_type = ? 
      AND {stat_column} IS NOT NULL 
      AND CAST({stat_column} AS INTEGER) > 0
    """
    
    params = [season_type]
    
    if season:
        base_query += " AND season = ?"
        params.append(season)
    
    base_query += f" ORDER BY CAST({stat_column} AS INTEGER) DESC LIMIT ?"
    params.append(limit)
    
    results = execute_query(base_query, tuple(params))
    
    if not results:
        return f"No data found for {stat_column}"
    
    season_text = f" - {season}" if season else ""
    response = f"Top {limit} in {stat_column.replace('_', ' ').title()}{season_text} ({season_type}):\n\n"
    
    for i, row in enumerate(results, 1):
        response += f"{i}. {row['team']} ({row['season']}): {row[stat_column]}\n"
    
    return response

@mcp.tool()
def compare_teams(team1: str, team2: str, season: int, season_type: str = "REG") -> str:
    """Compare statistics between two NFL teams for a specific season
    
    Args:
        team1: First team abbreviation (e.g., 'BUF')
        team2: Second team abbreviation (e.g., 'KC')
        season: Season year to compare
        season_type: Season type - 'REG' for regular season, 'POST' for postseason
    """
    team1 = team1.upper()
    team2 = team2.upper()
    
    query = """
    SELECT team, season, season_type, games,
           CAST(passing_yards AS INTEGER) as passing_yards,
           CAST(rushing_yards AS INTEGER) as rushing_yards,
           CAST(passing_tds AS INTEGER) as passing_tds,
           CAST(rushing_tds AS INTEGER) as rushing_tds,
           CAST(def_sacks AS INTEGER) as def_sacks,
           CAST(def_interceptions AS INTEGER) as def_interceptions,
           CAST(fg_pct AS REAL) as fg_pct
    FROM team_stats 
    WHERE team IN (?, ?) AND season = ? AND season_type = ?
    ORDER BY team
    """
    results = execute_query(query, (team1, team2, season, season_type))
    
    if len(results) != 2:
        return f"Could not find data for both teams in {season} ({season_type})"
    
    response = f"Team Comparison - {season} ({season_type}):\n\n"
    
    stats = ['passing_yards', 'rushing_yards', 'passing_tds', 'rushing_tds', 
            'def_sacks', 'def_interceptions', 'fg_pct']
    
    for stat in stats:
        response += f"{stat.replace('_', ' ').title()}:\n"
        for row in results:
            value = row.get(stat, 'N/A')
            if stat == 'fg_pct' and value != 'N/A':
                value = f"{value}%"
            response += f"  {row['team']}: {value}\n"
        response += "\n"
    
    return response

@mcp.tool()
def get_playoff_teams(season: int | None = None) -> str:
    """Get teams that made the playoffs (have postseason data)
    
    Args:
        season: Season year (optional, gets all seasons if not specified)
    """
    query = """
    SELECT season, team, games as playoff_games
    FROM team_stats 
    WHERE season_type = 'POST'
    """
    
    if season:
        query += " AND season = ?"
        results = execute_query(query, (season,))
    else:
        query += " ORDER BY season DESC, team"
        results = execute_query(query)
    
    if not results:
        return "No playoff data found" + (f" for {season}" if season else "")
    
    season_text = f" - {season}" if season else ""
    response = f"Playoff Teams{season_text}:\n\n"
    
    current_season = None
    for row in results:
        if row['season'] != current_season:
            current_season = row['season']
            response += f"\n{current_season}:\n"
        response += f"  {row['team']} ({row['playoff_games']} games)\n"
    
    return response

@mcp.tool()
def get_teams_by_season(season: int, season_type: str = "REG") -> str:
    """Get all teams and basic stats for a specific season
    
    Args:
        season: Season year
        season_type: Season type - 'REG' for regular season, 'POST' for postseason
    """
    query = """
    SELECT team, games, passing_yards, rushing_yards, def_sacks
    FROM team_stats 
    WHERE season = ? AND season_type = ?
    ORDER BY team
    """
    results = execute_query(query, (season, season_type))
    
    if not results:
        return f"No data found for {season} ({season_type})"
    
    response = f"Teams in {season} ({season_type}):\n\n"
    for row in results:
        response += f"{row['team']}: {row['games']} games, "
        response += f"{row['passing_yards']} pass yds, "
        response += f"{row['rushing_yards']} rush yds, "
        response += f"{row['def_sacks']} sacks\n"
    
    return response

def main():
    global DB_PATH
    
    # Check for environment variable first (for mcp dev compatibility)
    DB_PATH = os.environ.get('DB_PATH')
    
    if not DB_PATH:
        parser = argparse.ArgumentParser(description="NFL Team Stats MCP Server")
        parser.add_argument("--db", required=True, help="Path to SQLite database file")
        args = parser.parse_args()
        DB_PATH = args.db
    
    # Test database connection
    try:
        execute_query("SELECT COUNT(*) FROM team_stats")
        print(f"Connected to database: {DB_PATH}", file=sys.stderr)
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Run the server
    mcp.run()

if __name__ == "__main__":
    main()