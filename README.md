# NFL Team Stats - Downloader & MCP Server

Downloads NFL team statistics CSV files from the nflverse-data GitHub repository and stores them in a SQLite database, plus provides an MCP server for querying the data through Claude and other MCP clients.

## Features

### Data Downloader
- Downloads only CSV files (`stats_team_reg_YEAR.csv` and `stats_team_post_YEAR.csv`)
- Separates regular season and postseason data using `season_type` column
- Prevents duplicate downloads with built-in logging
- Supports filtering by specific years
- All 89+ statistical columns included

### MCP Server
- Query NFL team statistics through natural language with Claude
- 7 specialized tools for different types of queries
- Secure database access (read-only operations)
- Support for team comparisons, stat leaders, playoff teams, and more
- Custom SQL query execution for advanced users

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

- "Who led the NFL in passing yards in 2023?"
- "Compare the Bills and Chiefs offensive stats from 2023"
- "Which teams made the playoffs in 2022?"
- "Show me the top 5 rushing offenses from 2024 regular season"
- "What were the Packers' defensive stats in 2023?"

### Manual MCP Server Testing

```bash
# Test the server with MCP inspector
uv run mcp dev nfl_stats_server.py --db nfl_stats.db

# Or start the server directly
uv run nfl_stats_server.py --db nfl_stats.db
```

### Quick Test

Once you have data downloaded, test the server quickly:

```bash
# Download sample data first
uv run nfl_team_stats_downloader.py --db nfl_stats.db --years 2023

# Test with MCP dev tool
uv run mcp dev nfl_stats_server.py --db nfl_stats.db
```