# Google Maps Scraper

A robust, fault-tolerant Google Maps scraper that uses DataForSEO, Redis, and PostgreSQL. This project allows you to scrape business leads (names, addresses, websites, etc.) from Google Maps for specific keywords and locations.

## Features

- **Adaptive Grid Search**: Automatically splits geographic areas into smaller sub-squares if the number of results exceeds Google's limit (100), ensuring comprehensive coverage.
- **Reliable Queue**: Uses Redis for task management with a reliable queue pattern (RPOPLPUSH) to ensure no tasks are lost even if a worker crashes.
- **Fault Tolerance**: Includes a "Janitor" process to recover stalled tasks.
- **Data Persistence**: Stores results in a PostgreSQL database.
- **Export**: CLI command to export results to Excel.
- **Task Logging**: Automatically logs DataForSEO task IDs locally for future reference or re-retrieval.

## Prerequisites

- **Python 3.10+**
- **PostgreSQL** (running locally or accessible via network)
- **Redis** (running locally or accessible via network)
- **DataForSEO Account**: You need a login and password for the DataForSEO API.

## Installation

1.  **Clone the repository**.
2.  **Install dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

3.  **Create a `.env` file** in the root directory with your DataForSEO credentials and database configuration:

    ```env
    DATAFORSEO_LOGIN=your_login
    DATAFORSEO_PASSWORD=your_password

    # Optional overrides (defaults to localhost)
    # POSTGRES_HOST=localhost
    # POSTGRES_USER=user
    # POSTGRES_PASSWORD=password
    # POSTGRES_DB=scraper_db
    # REDIS_HOST=localhost
    ```

## Usage

### 1. Initialize the Database

The database needs to be initialized before the first run.

```bash
python main.py init
```

### 2. Start the Worker

Start one or more worker processes in separate terminals.

```bash
python worker.py
```

To increase concurrency, simply open more terminals and run `python worker.py` in each.

### 3. Seed Tasks

To start scraping, you need to seed the queue with a starting location and keywords.

```bash
python main.py seed --city "London" --keywords "plumber,electrician" --lat 51.5074 --lng -0.1278 --width 20000
```

- `--city`: Descriptive name for the logs.
- `--keywords`: Comma-separated list of keywords to search.
- `--lat`: Latitude of the center point (optional if city is geocodable).
- `--lng`: Longitude of the center point (optional if city is geocodable).
- `--width`: Width of the initial square in meters (default 20000 = 20km).

### 4. Monitor Progress

Check the output in the terminal where you are running `worker.py`.

### 5. Export Results

Once the queue is empty (or whenever you want to dump the data):

```bash
python main.py finish --output my_leads.xlsx
```

This will:
1.  Export the data to `my_leads.xlsx`.
2.  Ask if you want to flush (truncate) the database.

## Task ID Logging

The system is configured to save all DataForSEO task IDs locally. This is useful if you need to retrieve the raw data again later or debug specific API calls.

- **Location**: `data/task_ids.jsonl` (created in the current directory).
- **Format**: JSON Lines (one JSON object per line).
- **Content**:
  - `task_id`: The ID returned by DataForSEO.
  - `timestamp`: UTC timestamp of when the task was posted.
  - `keyword`: The keyword searched.
  - `lat`, `lng`, `width`: The geographic parameters.
  - `zoom`: The calculated zoom level used.

Example entry:
```json
{"task_id": "01234567-89ab-cdef-0123-456789abcdef", "timestamp": "2023-10-27T10:00:00.000000", "keyword": "plumber", "lat": 51.5074, "lng": -0.1278, "width": 20000, "zoom": 12}
```
