# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mcp[cli]>=1.0.0",
#   "requests>=2.31",
# ]
# ///
#!/usr/bin/env python3
"""
NFL Live MCP Server

A comprehensive MCP server that provides tools to query NFL historical data from SQLite databases
and live data from ESPN's API.

Usage:
    uv run nfl_live_server.py --team-stats-db nfl_stats.db --plays-db nfl_plays.db --scores-db nfl_scores.db
    
    Or for Claude Desktop, add to your config:
    {
      "mcpServers": {
        "nfl-live": {
          "command": "uv",
          "args": ["run", "nfl_live_server.py", 
                   "--team-stats-db", "nfl_stats.db", 
                   "--plays-db", "nfl_plays.db", 
                   "--scores-db", "nfl_scores.db"],
          "cwd": "/path/to/this/directory"
        }
      }
    }
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from mcp.server.fastmcp import FastMCP

# Global database paths
TEAM_STATS_DB: str = ""
PLAYS_DB: str = ""
SCORES_DB: str = ""

# ESPN API endpoint
ESPN_SCOREBOARD_URL = "https://cdn.espn.com/core/nfl/scoreboard"

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

def fetch_live_scoreboard() -> Dict[str, Any]:
    """Fetch live NFL scoreboard data from ESPN API."""
    try:
        response = requests.get(
            ESPN_SCOREBOARD_URL,
            params={"xhr": "1", "limit": "50"},
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json"
            },
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise Exception(f"ESPN API error: {str(e)}")

# Create the MCP server
mcp = FastMCP("NFL Live Stats & Scores")

@mcp.tool()
def get_live_scores() -> str:
    """Get current live NFL scores and game status from ESPN"""
    try:
        data = fetch_live_scoreboard()
        
        # ESPN API has events under content.sbData.events
        events = data.get('content', {}).get('sbData', {}).get('events', [])
        
        if not events:
            return "No NFL games found in the current schedule"
        
        response = f"🏈 Live NFL Scores & Status\n"
        response += f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        games = events
        
        # Group games by status
        live_games = []
        upcoming_games = []
        final_games = []
        
        for game in games:
            status = game.get('status', {}).get('type', {}).get('name', 'Unknown')
            
            if status in ['STATUS_IN_PROGRESS', 'STATUS_HALFTIME']:
                live_games.append(game)
            elif status in ['STATUS_SCHEDULED', 'STATUS_POSTPONED']:
                upcoming_games.append(game)
            else:
                final_games.append(game)
        
        # Show live games first
        if live_games:
            response += "🔴 LIVE GAMES:\n"
            for game in live_games:
                response += format_game_summary(game) + "\n"
        
        # Show final games
        if final_games:
            response += "\n✅ FINAL SCORES:\n"
            for game in final_games:
                response += format_game_summary(game) + "\n"
        
        # Show upcoming games
        if upcoming_games:
            response += "\n⏰ UPCOMING GAMES:\n"
            for game in upcoming_games:
                response += format_game_summary(game) + "\n"
        
        return response
        
    except Exception as e:
        return f"Error fetching live scores: {str(e)}"

@mcp.tool()
def get_live_game_details(team1: str, team2: str = None) -> str:
    """Get detailed information about a specific live or recent NFL game
    
    Args:
        team1: First team name or abbreviation (e.g., 'Bills', 'BUF')
        team2: Second team name or abbreviation (optional, will search for team1's current game if not provided)
    """
    try:
        data = fetch_live_scoreboard()
        
        # ESPN API has events under content.sbData.events
        events = data.get('content', {}).get('sbData', {}).get('events', [])
        
        if not events:
            return "No NFL games found in the current schedule"
        
        # Normalize team names for searching
        team1 = team1.upper()
        team2 = team2.upper() if team2 else None
        
        target_game = None
        
        # Search for the game
        for game in events:
            competitions = game.get('competitions', [])
            if not competitions:
                continue
                
            competition = competitions[0]
            competitors = competition.get('competitors', [])
            
            if len(competitors) < 2:
                continue
            
            team_names = []
            team_abbrevs = []
            
            for competitor in competitors:
                team = competitor.get('team', {})
                team_names.append(team.get('name', '').upper())
                team_names.append(team.get('displayName', '').upper())
                team_abbrevs.append(team.get('abbreviation', '').upper())
            
            # Check if team1 is in this game
            if team1 in team_names or team1 in team_abbrevs:
                if team2 is None:
                    # Found team1's game
                    target_game = game
                    break
                elif team2 in team_names or team2 in team_abbrevs:
                    # Found both teams
                    target_game = game
                    break
        
        if not target_game:
            search_term = f"{team1} vs {team2}" if team2 else team1
            return f"No current game found for {search_term}"
        
        return format_detailed_game(target_game)
        
    except Exception as e:
        return f"Error fetching game details: {str(e)}"

@mcp.tool()
def get_nfl_standings() -> str:
    """Get current NFL standings and playoff picture"""
    # Note: ESPN scoreboard doesn't include standings, but we can infer from recent games
    # This is a simplified version - a full implementation would use a dedicated standings API
    try:
        data = fetch_live_scoreboard()
        
        response = "📊 NFL Season Information\n\n"
        
        # Get season info from content.sbData
        season_info = data.get('content', {}).get('sbData', {}).get('leagues', [{}])[0].get('season', {})
        if season_info:
            response += f"Season: {season_info.get('year', 'N/A')}\n"
            response += f"Type: {season_info.get('type', {}).get('name', 'N/A')}\n"
            response += f"Week: {season_info.get('week', {}).get('number', 'N/A')}\n\n"
        
        response += "For complete standings, please check ESPN.com or NFL.com\n"
        response += "This tool focuses on live scores and game details.\n"
        
        # Show some recent results to give context
        events = data.get('content', {}).get('sbData', {}).get('events', [])
        if events:
            response += "\n📈 Recent Results:\n"
            recent_finals = [g for g in events 
                           if g.get('status', {}).get('type', {}).get('name') == 'STATUS_FINAL'][:5]
            
            for game in recent_finals:
                response += format_game_summary(game, show_date=True) + "\n"
        
        return response
        
    except Exception as e:
        return f"Error fetching NFL information: {str(e)}"

def format_game_summary(game: Dict[str, Any], show_date: bool = False) -> str:
    """Format a game into a summary string."""
    try:
        competition = game.get('competitions', [{}])[0]
        competitors = competition.get('competitors', [])
        
        if len(competitors) < 2:
            return "Invalid game data"
        
        # Get teams (away is typically index 1, home is index 0)
        away_team = competitors[1].get('team', {}) if len(competitors) > 1 else {}
        home_team = competitors[0].get('team', {}) if len(competitors) > 0 else {}
        
        away_score = competitors[1].get('score', '0') if len(competitors) > 1 else '0'
        home_score = competitors[0].get('score', '0') if len(competitors) > 0 else '0'
        
        # Game status
        status = game.get('status', {})
        status_name = status.get('type', {}).get('name', 'Unknown')
        status_detail = status.get('type', {}).get('detail', '')
        
        # Format the summary
        away_name = away_team.get('abbreviation', 'TBD')
        home_name = home_team.get('abbreviation', 'TBD')
        
        if status_name == 'STATUS_SCHEDULED':
            game_str = f"{away_name} @ {home_name}"
            if show_date:
                game_date = game.get('date', '')
                if game_date:
                    try:
                        dt = datetime.fromisoformat(game_date.replace('Z', '+00:00'))
                        game_str += f" ({dt.strftime('%m/%d %H:%M')})"
                    except:
                        pass
            game_str += f" - {status_detail}"
        else:
            game_str = f"{away_name} {away_score} - {home_name} {home_score}"
            
            if status_name == 'STATUS_IN_PROGRESS':
                period = status.get('period', 1)
                clock = status.get('displayClock', '')
                game_str += f" ({period}Q {clock})"
            elif status_name == 'STATUS_HALFTIME':
                game_str += " (HALFTIME)"
            elif status_name == 'STATUS_FINAL':
                if status_detail and status_detail != 'Final':
                    game_str += f" ({status_detail})"
                else:
                    game_str += " (FINAL)"
        
        return game_str
        
    except Exception:
        return "Error formatting game"

def format_detailed_game(game: Dict[str, Any]) -> str:
    """Format detailed game information."""
    try:
        competition = game.get('competitions', [{}])[0]
        competitors = competition.get('competitors', [])
        
        if len(competitors) < 2:
            return "Invalid game data"
        
        # Get teams
        away_team = competitors[1] if len(competitors) > 1 else {}
        home_team = competitors[0] if len(competitors) > 0 else {}
        
        response = "🏈 Game Details\n\n"
        
        # Teams and scores
        away_info = away_team.get('team', {})
        home_info = home_team.get('team', {})
        
        response += f"{away_info.get('displayName', 'Away Team')} ({away_info.get('abbreviation', 'AWAY')})\n"
        response += f"vs\n"
        response += f"{home_info.get('displayName', 'Home Team')} ({home_info.get('abbreviation', 'HOME')})\n\n"
        
        # Score
        away_score = away_team.get('score', '0')
        home_score = home_team.get('score', '0')
        response += f"Score: {away_info.get('abbreviation', 'AWAY')} {away_score} - {home_info.get('abbreviation', 'HOME')} {home_score}\n\n"
        
        # Game status
        status = game.get('status', {})
        status_name = status.get('type', {}).get('name', 'Unknown')
        status_detail = status.get('type', {}).get('detail', '')
        
        if status_name == 'STATUS_IN_PROGRESS':
            period = status.get('period', 1)
            clock = status.get('displayClock', '')
            response += f"Status: {period}Q {clock} - IN PROGRESS 🔴\n"
        elif status_name == 'STATUS_HALFTIME':
            response += f"Status: HALFTIME\n"
        elif status_name == 'STATUS_FINAL':
            response += f"Status: FINAL\n"
        else:
            response += f"Status: {status_detail}\n"
        
        # Date and venue
        game_date = game.get('date', '')
        if game_date:
            try:
                dt = datetime.fromisoformat(game_date.replace('Z', '+00:00'))
                response += f"Date: {dt.strftime('%A, %B %d, %Y at %I:%M %p UTC')}\n"
            except:
                response += f"Date: {game_date}\n"
        
        # Venue
        venue = competition.get('venue', {})
        if venue:
            response += f"Venue: {venue.get('fullName', 'N/A')}\n"
            address = venue.get('address', {})
            if address:
                city = address.get('city', '')
                state = address.get('state', '')
                if city and state:
                    response += f"Location: {city}, {state}\n"
        
        # Broadcast info
        broadcasts = competition.get('broadcasts', [])
        if broadcasts:
            networks = [b.get('media', {}).get('shortName', '') for b in broadcasts if b.get('media', {}).get('shortName')]
            if networks:
                response += f"TV: {', '.join(networks)}\n"
        
        # Betting odds (if available)
        odds = competition.get('odds', [])
        if odds:
            response += f"\n📊 Betting Info:\n"
            for odd in odds[:1]:  # Just show first odds source
                details = odd.get('details', '')
                over_under = odd.get('overUnder', '')
                if details:
                    response += f"Spread: {details}\n"
                if over_under:
                    response += f"Over/Under: {over_under}\n"
        
        # Team records
        away_record = away_team.get('records', [])
        home_record = home_team.get('records', [])
        
        if away_record or home_record:
            response += f"\n📈 Records:\n"
            if away_record:
                for record in away_record:
                    if record.get('type') == 'total':
                        response += f"{away_info.get('abbreviation', 'AWAY')}: {record.get('summary', 'N/A')}\n"
            if home_record:
                for record in home_record:
                    if record.get('type') == 'total':
                        response += f"{home_info.get('abbreviation', 'HOME')}: {record.get('summary', 'N/A')}\n"
        
        return response
        
    except Exception as e:
        return f"Error formatting game details: {str(e)}"

# Include historical data tools from the comprehensive server
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
    
    response += f"\n🔴 Live Data: ESPN API integration available\n"
    
    return response

@mcp.tool()
def get_game_plays(away_team: str, home_team: str, season: int, week: str = None) -> str:
    """Get all plays from a specific historical game (from database)
    
    Args:
        away_team: Away team abbreviation (e.g., 'BUF', 'KC')
        home_team: Home team abbreviation (e.g., 'BUF', 'KC')
        season: Season year
        week: Week number or description (optional, gets first match if not specified)
    """
    if not PLAYS_DB or not os.path.exists(PLAYS_DB):
        return "Plays database not available"
    
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
    
    base_query += " ORDER BY quarter, drive_number, play_number_in_drive LIMIT 200"
    
    try:
        results = execute_query(PLAYS_DB, base_query, tuple(params))
        
        if not results:
            return f"No plays found for {away_team} @ {home_team} in {season}" + (f" week {week}" if week else "")
        
        response = f"Historical Game Plays: {away_team} @ {home_team} - {season}" + (f" Week {week}" if week else "") + f"\n\n"
        response += f"Total plays: {len(results)}\n\n"
        
        current_quarter = None
        scoring_plays = []
        
        for play in results:
            if play['quarter'] != current_quarter:
                current_quarter = play['quarter']
                response += f"\n=== {current_quarter} ===\n"
            
            if play['is_scoring_play']:
                scoring_plays.append(play)
            
            scoring_flag = " 🏈" if play['is_scoring_play'] else ""
            response += f"  {play['team_with_possession']}: {play['play_outcome']}{scoring_flag}\n"
        
        if scoring_plays:
            response += f"\n🏈 Scoring Plays ({len(scoring_plays)}):\n"
            for play in scoring_plays:
                response += f"  {play['quarter']} - {play['team_with_possession']}: {play['play_outcome']}\n"
        
        return response
        
    except Exception as e:
        return f"Error retrieving plays: {str(e)}"

@mcp.tool()
def get_team_season_record(team: str, season: int) -> str:
    """Get a team's complete season record from historical database
    
    Args:
        team: Team abbreviation (e.g., 'BUF', 'KC', 'GB')
        season: Season year
    """
    if not SCORES_DB or not os.path.exists(SCORES_DB):
        return "Scores database not available"
    
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
    LIMIT 20
    """
    
    try:
        results = execute_query(SCORES_DB, query, (team, team, team, team, team, team, team, season))
        
        if not results:
            return f"No games found for {team} in {season}"
        
        # Calculate record
        regular_wins = sum(1 for game in results if game['team_won'] and not game['post_season'])
        regular_losses = sum(1 for game in results if not game['team_won'] and not game['post_season'])
        
        response = f"{team} {season} Historical Season Record\n\n"
        response += f"Regular Season: {regular_wins}-{regular_losses}\n"
        
        response += f"\nRecent Games:\n"
        for game in results[:10]:  # Show up to 10 games
            result = "W" if game['team_won'] else "L"
            location = "vs" if game['home_away'] == 'Home' else "@"
            
            response += f"  Week {game['week']:<12} {result} {location} {game['opponent']:<3} "
            response += f"{game['team_score']:.0f}-{game['opponent_score']:.0f}\n"
        
        return response
        
    except Exception as e:
        return f"Error retrieving season record: {str(e)}"

def main():
    global TEAM_STATS_DB, PLAYS_DB, SCORES_DB
    
    # Check for environment variables first (for mcp dev compatibility)
    TEAM_STATS_DB = os.environ.get('TEAM_STATS_DB', '')
    PLAYS_DB = os.environ.get('PLAYS_DB', '')
    SCORES_DB = os.environ.get('SCORES_DB', '')
    
    if not (TEAM_STATS_DB and PLAYS_DB and SCORES_DB):
        parser = argparse.ArgumentParser(description="NFL Live MCP Server")
        parser.add_argument("--team-stats-db", help="Path to team stats SQLite database file")
        parser.add_argument("--plays-db", help="Path to plays SQLite database file") 
        parser.add_argument("--scores-db", help="Path to scores SQLite database file")
        args = parser.parse_args()
        
        TEAM_STATS_DB = args.team_stats_db or TEAM_STATS_DB
        PLAYS_DB = args.plays_db or PLAYS_DB
        SCORES_DB = args.scores_db or SCORES_DB
    
    # Note: We don't require databases to exist since we have live API functionality
    available_sources = ["ESPN Live API"]
    
    if TEAM_STATS_DB and os.path.exists(TEAM_STATS_DB):
        available_sources.append("team stats DB")
    if PLAYS_DB and os.path.exists(PLAYS_DB):
        available_sources.append("plays DB")
    if SCORES_DB and os.path.exists(SCORES_DB):
        available_sources.append("scores DB")
    
    print(f"NFL Live Server - Available data sources: {', '.join(available_sources)}", file=sys.stderr)
    
    # Run the server
    mcp.run()

if __name__ == "__main__":
    main()