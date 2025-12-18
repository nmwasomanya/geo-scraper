import math
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from geopy.distance import geodesic

def calculate_circumscribed_radius(width_meters: float) -> int:
    """
    Calculates the API Request Radius to circumscribe a square.
    Radius = (Width * sqrt(2)) / 2
    """
    radius = (width_meters * math.sqrt(2)) / 2
    return math.ceil(radius)

def calculate_zoom_level(radius_meters: float, lat: float) -> int:
    """
    Calculates the approximate Zoom Level for a given radius.
    Formula derived from the fact that map width in meters is roughly:
    MapWidth = 40,075,000 * cos(lat) / 2^zoom
    We assume the viewport width we want covers 2 * radius.
    2 * radius = 40,075,000 * cos(lat) / 2^zoom
    2^zoom = 40,075,000 * cos(lat) / (2 * radius)
    zoom = log2( 40,075,000 * cos(lat) / (2 * radius) )
    """
    if radius_meters <= 0:
        return 21 # Max zoom

    circumference = 40075000 * math.cos(math.radians(lat))
    desired_width = 2 * radius_meters

    if desired_width <= 0:
        return 21

    zoom = math.log2(circumference / desired_width)
    return math.floor(zoom)

def split_square(lat: float, lng: float, width_meters: float):
    """
    Splits a square defined by center (lat, lng) and width into 4 sub-squares.
    Returns a list of 4 tuples: (new_lat, new_lng, new_width).

    Approximation:
    1 degree of latitude ~= 111,000 meters.
    1 degree of longitude ~= 111,000 * cos(latitude) meters.
    """

    # New width is half the current width
    new_width = width_meters / 2

    # Offset in meters from the center to the sub-square centers
    offset = new_width / 2

    # Convert offset meters to degrees
    lat_offset = offset / 111000
    lng_offset = offset / (111000 * math.cos(math.radians(lat)))

    sub_squares = [
        (lat + lat_offset, lng + lng_offset, new_width), # Top-Right
        (lat + lat_offset, lng - lng_offset, new_width), # Top-Left
        (lat - lat_offset, lng + lng_offset, new_width), # Bottom-Right
        (lat - lat_offset, lng - lng_offset, new_width), # Bottom-Left
    ]

    return sub_squares

def get_city_info(city_name: str):
    """
    Geocodes a city name to get latitude, longitude, and an estimated width in meters
    based on its bounding box.

    Returns:
        tuple: (lat, lng, width_meters) or (None, None, None) on failure.
    """
    geolocator = Nominatim(user_agent="city_scraper_cli")
    try:
        location = geolocator.geocode(city_name)
        if location:
            lat = location.latitude
            lng = location.longitude

            # Bounding box: [min_latitude, max_latitude, min_longitude, max_longitude]
            raw = location.raw
            boundingbox = raw.get('boundingbox')
            width_meters = 20000.0 # Default fallback

            if boundingbox:
                 # Nominatim returns strings in the list
                 min_lat = float(boundingbox[0])
                 max_lat = float(boundingbox[1])
                 min_lon = float(boundingbox[2])
                 max_lon = float(boundingbox[3])

                 # Width: distance between min_lon and max_lon at the center latitude
                 # We use geodesic distance which is more accurate
                 width_meters_lon = geodesic((lat, min_lon), (lat, max_lon)).meters

                 # Height: distance between min_lat and max_lat at the center longitude
                 height_meters_lat = geodesic((min_lat, lng), (max_lat, lng)).meters

                 # We take the larger dimension to ensure coverage
                 width_meters = max(width_meters_lon, height_meters_lat)

            return lat, lng, width_meters
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"Geocoding error: {e}")
        return None, None, None
    except Exception as e:
        print(f"Unexpected error during geocoding: {e}")
        return None, None, None

    return None, None, None
