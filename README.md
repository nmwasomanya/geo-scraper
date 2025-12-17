# Google Maps Scraper

A robust, fault-tolerant Google Maps scraper that uses DataForSEO, Redis, PostgreSQL, and Docker. This project allows you to scrape business leads (names, addresses, websites, etc.) from Google Maps for specific keywords and locations.

## Features

- **Distributed Architecture**: Uses Docker Compose to orchestrate 8 worker replicas (configurable) to parallelize scraping.
- **Adaptive Grid Search**: Automatically splits geographic areas into smaller sub-squares if the number of results exceeds Google's limit (100), ensuring comprehensive coverage.
- **Reliable Queue**: Uses Redis for task management with a reliable queue pattern (RPOPLPUSH) to ensure no tasks are lost even if a worker crashes.
- **Fault Tolerance**: Includes a "Janitor" process to recover stalled tasks.
- **Data Persistence**: Stores results in a PostgreSQL database.
- **Export**: CLI command to export results to Excel.
- **Task Logging**: Automatically logs DataForSEO task IDs locally for future reference or re-retrieval.

## Prerequisites

- **Docker** and **Docker Compose** installed.
- **DataForSEO Account**: You need a login and password for the DataForSEO API.

## Project Structure

- `main.py`: CLI entry point for seeding tasks and exporting data.
- `worker.py`: Async worker that processes tasks from Redis, calls DataForSEO, and saves results to DB.
- `queue_manager.py`: Handles Redis queue operations (push, pop, complete, recovery).
- `models.py`: SQLAlchemy database models.
- `geo_utils.py`: Geographic utility functions (grid splitting, zoom calculation).
- `exporter.py`: Logic to export data to Excel.
- `docker-compose.yml`: Defines the services (db, redis, worker, app).

## Setup & Installation

1.  **Clone the repository**.
2.  **Create a `.env` file** in the root directory with your DataForSEO credentials:

    ```env
    DATAFORSEO_LOGIN=your_login
    DATAFORSEO_PASSWORD=your_password
    ```

3.  **Build and start the services**:

    ```bash
    docker-compose up -d --build
    ```

    This will start:
    - A PostgreSQL database (`db`).
    - A Redis instance (`redis`).
    - 8 Worker containers (`worker`).
    - An App container (`app`) kept alive for running CLI commands.

## Usage

### 1. Initialize the Database

The database is initialized automatically on the first seed run, or you can run:

```bash
docker-compose exec app python main.py init
```

### 2. Seed Tasks

To start scraping, you need to seed the queue with a starting location and keywords.

```bash
docker-compose exec app python main.py seed --city "London" --keywords "plumber,electrician" --lat 51.5074 --lng -0.1278 --width 20000
```

- `--city`: Descriptive name for the logs.
- `--keywords`: Comma-separated list of keywords to search.
- `--lat`: Latitude of the center point.
- `--lng`: Longitude of the center point.
- `--width`: Width of the initial square in meters (default 20000 = 20km).

### 3. Monitor Progress

You can check the logs of the workers:

```bash
docker-compose logs -f worker
```

### 4. Export Results

Once the queue is empty (or whenever you want to dump the data):

```bash
docker-compose exec app python main.py finish --output my_leads.xlsx
```

This will:
1.  Export the data to `my_leads.xlsx` inside the container.
2.  Ask if you want to flush (truncate) the database.

Note: Since the `app` container has a volume mount at `/app/data`, you might want to modify the output path to save it there:

```bash
docker-compose exec app python main.py finish --output /app/data/my_leads.xlsx
```

Then you can find `my_leads.xlsx` in your local `data/` folder on your host machine.

## Task ID Logging

The system is configured to save all DataForSEO task IDs locally. This is useful if you need to retrieve the raw data again later or debug specific API calls.

- **Location**: `data/task_ids.jsonl` (in the root of the project on your host machine).
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

## Scaling

To change the number of workers, update the `replicas` count in `docker-compose.yml` under the `worker` service and re-run `docker-compose up -d`.
