import typer
import os
import json
from geo_utils import split_square # Or just logic to create initial square
from queue_manager import QueueManager
from exporter import export_to_excel, flush_database
from models import init_db

app = typer.Typer()
queue = QueueManager()

@app.command()
def seed(
    city: str = typer.Option(..., help="City name (for logging/reference)"),
    keywords: str = typer.Option(..., help="Comma-separated keywords"),
    lat: float = typer.Option(51.5074, help="Latitude of center"),
    lng: float = typer.Option(-0.1278, help="Longitude of center"),
    width: float = typer.Option(20000, help="Width of square in meters")
):
    """
    Seeds the queue with initial tasks.
    Default lat/lng is London. width in meters (20km).
    """
    init_db()
    print(f"Seeding for city: {city}, keywords: {keywords}")

    # We create an initial task.
    # We might need to geocode the city to get lat/lng if not provided,
    # but for this CLI we accept lat/lng or default to London.
    # The user example: seed --city "London" --keywords "gym"

    # If the user meant for us to geocode "London", we would need a geocoding lib or API.
    # For simplicity, we assume the user might provide lat/lng or we default to a known point.
    # Let's use the provided default or arguments.

    keyword_list = [k.strip() for k in keywords.split(",")]

    for kw in keyword_list:
        task = {
            "lat": lat,
            "lng": lng,
            "width": width,
            "keyword": kw
        }
        queue.push_task(task)
        print(f"Pushed task: {task}")

@app.command()
def finish(output: str = "my_leads.xlsx"):
    """
    Exports data to Excel and flushes the database.
    """
    print("Starting export process...")
    success, filename = export_to_excel(output)

    if success:
        print(f"Export successful: {filename}")
        confirm = input("Confirm database flush? (y/n): ")
        if confirm.lower() == 'y':
            flush_database()
        else:
            print("Flush aborted.")
    else:
        print("Export failed. Database not flushed.")

@app.command()
def init():
    """Initialize DB schema."""
    init_db()
    print("Database initialized.")

if __name__ == "__main__":
    app()
