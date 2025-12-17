import typer
import os
import json
from geo_utils import split_square, get_city_info
from queue_manager import QueueManager
from exporter import export_to_excel, flush_database
from models import init_db
from typing import Optional

app = typer.Typer()
queue = QueueManager()

@app.command()
def seed(city: str, keywords: str, lat: Optional[float] = None, lng: Optional[float] = None, width: Optional[float] = None):
    """
    Seeds the queue with initial tasks.
    If lat/lng are not provided, tries to geocode the city.
    If width is not provided, tries to estimate it from the city's bounding box or defaults to 20km.
    """
    init_db()
    print(f"Seeding for city: {city}, keywords: {keywords}")

    # Determine lat/lng/width
    if lat is None or lng is None:
        print(f"Geocoding city: {city}...")
        c_lat, c_lng, c_width = get_city_info(city)

        if c_lat is not None and c_lng is not None:
            lat = lat if lat is not None else c_lat
            lng = lng if lng is not None else c_lng
            # If width was not provided by user, use the geocoded width
            if width is None:
                width = c_width
            print(f"Geocoded: lat={lat}, lng={lng}, width={width:.0f}m")
        else:
            # Fallback if geocoding failed and user didn't provide lat/lng
            if lat is None: lat = 51.5074 # Default London
            if lng is None: lng = -0.1278 # Default London
            print(f"Could not geocode '{city}'. Using defaults/provided: lat={lat}, lng={lng}")

    if width is None:
        width = 20000 # Default 20km if still None

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
