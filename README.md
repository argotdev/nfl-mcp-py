# NFL Team Stats - Downloader & MCP Server

Downloads NFL team statistics CSV files from the nflverse-data GitHub repository and stores them in a SQLite database, plus provides an MCP server for querying the data through Claude and other MCP clients.

## Features

### Data Downloader
- Downloads only CSV files (`stats_team_reg_YEAR.csv` and `stats_team_post_YEAR.csv`)
- Separates regular season and postseason data using `season_type` column
- Prevents duplicate downloads with built-in logging
- Supports filtering by specific years
- All 89+ statistical columns included

### CSV Data Importer
- Imports all play-by-play and game scores CSV files into separate SQLite databases
- Handles large datasets efficiently with chunked processing
- Creates optimized database schemas with proper indexing
- Supports data from 2020-2025 seasons (262k+ plays, 2k+ games)

### MCP Servers
#### Team Stats Server (`nfl_stats_server.py`)
- Query NFL team statistics through natural language with Claude
- 6 specialized tools for team stats queries
- Secure database access (read-only operations)

#### Comprehensive Server (`nfl_comprehensive_server.py`)
- **Complete NFL data access**: team stats, plays, and game scores
- **8+ specialized tools** including:
  - Game-by-game play analysis
  - Season records and playoff results
  - Play outcome searches
  - Team statistics across multiple databases
- **Multi-database support**: Works with any combination of the databases

#### Live Server (`nfl_live_server.py`) ⭐ **RECOMMENDED**
- **Everything from Comprehensive Server PLUS live data**
- **ESPN API integration** for real-time scores and game status
- **Live game tracking** with current scores, time remaining, and game situations
- **Current season information** and up-to-the-minute NFL updates
- **Historical + Live combined**: Ask about past games and current games in the same conversation

## Setup with uv (Recommended)

### Run as standalone scripts (simplest approach)
```bash
# Download NFL team stats data
uv run nfl_team_stats_downloader.py --db nfl_stats.db --years 2023 2024

# Run the MCP server
uv run nfl_stats_mcp_server.py --db nfl_stats.db
```

### Alternative: Use virtual environment
```bash
# Install dependencies in virtual environment
uv sync

# Activate environment and run scripts
uv shell
python nfl_team_stats_downloader.py --db nfl_stats.db --years 2023 2024
python nfl_stats_mcp_server.py --db nfl_stats.db
```

## Usage

### Basic usage
```bash
# Download all available years
uv run nfl_team_stats_downloader.py --db nfl_stats.db

# Download specific years
uv run nfl_team_stats_downloader.py --db nfl_stats.db --years 2020 2021 2022 2023 2024

# With GitHub token for higher rate limits
uv run nfl_team_stats_downloader.py --db nfl_stats.db --years 2023 2024 --github-token YOUR_TOKEN
```

### Command-line options
- `--db`: SQLite database path (required)
- `--years`: Specific years to download (optional, downloads all if not specified)
- `--github-token`: GitHub API token for higher rate limits (optional)

## Database Schema

### team_stats table
- Primary key: `(season, team, season_type)`
- `season_type`: 'REG' for regular season, 'POST' for postseason
- Contains all 89+ statistical columns including passing, rushing, receiving, defensive stats, etc.

### download_log table
- Tracks downloaded files to prevent duplicates
- Includes SHA256 checksums and row counts

## Requirements

- Python 3.10+
- pandas >= 2.0
- requests >= 2.31

## Installation without uv

If you prefer not to use uv:

```bash
pip install pandas requests mcp
python nfl_team_stats_downloader.py --db nfl_stats.db --years 2023 2024
python nfl_stats_mcp_server.py --db nfl_stats.db
```

## MCP Server Setup

### For Claude Desktop

1. **Download and prepare data:**
```bash
uv run nfl_team_stats_downloader.py --db nfl_stats.db --years 2020 2021 2022 2023 2024
```

2. **Add to Claude Desktop configuration:**

Copy the contents of `claude_desktop_config.json` to your Claude Desktop MCP settings, or manually add:

```json
{
  "mcpServers": {
    "nfl-team-stats": {
      "command": "uv",
      "args": [
        "run", 
        "nfl_stats_server.py",
        "--db",
        "nfl_stats.db"
      ],
      "cwd": "/path/to/your/nfl-mcp-py"
    }
  }
}
```

3. **Restart Claude Desktop** and you'll see the NFL stats tools available.

### MCP Server Tools

The server provides 7 tools for querying NFL team statistics:

1. **`get_data_overview`** - Overview of available data in the database
2. **`get_team_stats`** - Detailed statistics for a specific team
3. **`get_stat_leaders`** - Top performers in any statistical category
4. **`compare_teams`** - Head-to-head comparison between two teams
5. **`get_playoff_teams`** - Teams that made the playoffs
6. **`get_teams_by_season`** - All teams and basic stats for a season
7. **`execute_custom_query`** - Run custom SQL queries (advanced users)

### Example Queries

Once connected to Claude Desktop, you can ask questions like:

**Live/Current:**
- "What are the current NFL scores?"
- "Is there a game happening right now?"
- "Show me details about the Chiefs game today"
- "What's the current NFL season and week?"

**Historical Analysis:**
- "Who led the NFL in passing yards in 2023?"
- "Show me all the plays from the Bills vs Chiefs playoff game in 2024"
- "Compare the Bills and Chiefs offensive stats from 2023"
- "Which teams made the playoffs in 2022?"
- "Find all touchdown passes by Mahomes in 2023"

**Combined Historical + Live:**
- "How are the Chiefs doing this season compared to 2023?"
- "Show me the Eagles' record this year and their current game status"

### Manual MCP Server Testing

```bash
# Test the server with MCP inspector
uv run mcp dev nfl_stats_server.py --db nfl_stats.db

# Or start the server directly
uv run nfl_stats_server.py --db nfl_stats.db
```

### Quick Test

```bash
# 1. Download team stats data
uv run nfl_team_stats_downloader.py --db nfl_stats.db --years 2023

# 2. Import plays and scores data (if you have CSV files in nfl_stats directory)
uv run nfl_csv_importer.py

# 3. Test the live server (RECOMMENDED - includes everything + live ESPN data)
uv run nfl_live_server.py --team-stats-db nfl_stats.db --plays-db nfl_plays.db --scores-db nfl_scores.db

# Or test the comprehensive server (historical data only)
uv run nfl_comprehensive_server.py --team-stats-db nfl_stats.db --plays-db nfl_plays.db --scores-db nfl_scores.db

# Or test just team stats
uv run nfl_stats_server.py --db nfl_stats.db
```