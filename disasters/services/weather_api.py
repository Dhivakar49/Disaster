"""Fetches severe weather data from OpenWeatherMap API."""
import hashlib
import logging
from datetime import datetime

import requests
from django.conf import settings
from django.utils import timezone

from disasters.models import WeatherAlert, DataFetchLog, EarthquakeEvent

logger = logging.getLogger(__name__)

WEATHER_API_URL = getattr(
    settings, 'WEATHER_API_URL',
    'https://api.openweathermap.org/data/2.5/weather'
)
WEATHER_API_KEY = getattr(settings, 'WEATHER_API_KEY', '')
FETCH_TIMEOUT = 30

# Locations to monitor — expandable via settings
DEFAULT_MONITOR_LOCATIONS = [
    {'name': 'Kerala', 'lat': 10.8505, 'lon': 76.2711},
    {'name': 'Chennai', 'lat': 13.0827, 'lon': 80.2707},
    {'name': 'Mumbai', 'lat': 19.0760, 'lon': 72.8777},
    {'name': 'Delhi', 'lat': 28.7041, 'lon': 77.1025},
    {'name': 'Kolkata', 'lat': 22.5726, 'lon': 88.3639},
    {'name': 'Bangalore', 'lat': 12.9716, 'lon': 77.5946},
    {'name': 'Hyderabad', 'lat': 17.3850, 'lon': 78.4867},
    {'name': 'Ahmedabad', 'lat': 23.0225, 'lon': 72.5714},
    {'name': 'Pune', 'lat': 18.5204, 'lon': 73.8567},
    {'name': 'Jaipur', 'lat': 26.9124, 'lon': 75.7873},
    {'name': 'Lucknow', 'lat': 26.8467, 'lon': 80.9462},
    {'name': 'Bhopal', 'lat': 23.2599, 'lon': 77.4126},
    {'name': 'Visakhapatnam', 'lat': 17.6868, 'lon': 83.2185},
    {'name': 'Guwahati', 'lat': 26.1445, 'lon': 91.7362},
    {'name': 'Patna', 'lat': 25.6093, 'lon': 85.1376},
]

# Thresholds for flagging extreme weather
EXTREME_THRESHOLDS = {
    'wind_speed': 20.0,      # m/s (~72 km/h)
    'rainfall_1h': 50.0,     # mm/h (heavy rain)
    'temp_high': 45.0,       # °C
    'temp_low': -10.0,       # °C
    'humidity_high': 95.0,   # %
    'pressure_low': 990.0,   # hPa (potential cyclone)
}


def _get_disaster_locations():
    """
    Build dynamic locations from earthquake events and EONET events in the DB.
    Rounds coordinates to 1 decimal to avoid fetching weather for nearly identical spots.
    """
    seen = set()
    locations = []

    # From earthquakes (recent ones with valid coordinates)
    for eq in EarthquakeEvent.objects.filter(
        latitude__isnull=False, longitude__isnull=False
    ).order_by('-event_time')[:200]:
        key = (round(eq.latitude, 1), round(eq.longitude, 1))
        if key not in seen:
            seen.add(key)
            name = eq.place if eq.place else f"EQ:{eq.latitude:.2f},{eq.longitude:.2f}"
            locations.append({'name': name, 'lat': eq.latitude, 'lon': eq.longitude})

    return locations


def fetch_weather_data():
    """
    Fetch current weather for hardcoded + disaster locations.
    Returns (new_count, total_fetched) tuple.
    """
    if not WEATHER_API_KEY:
        logger.warning("WEATHER_API_KEY not configured — skipping weather fetch")
        log = DataFetchLog(source='weather', success=False,
                           error_message='WEATHER_API_KEY not configured')
        log.save()
        return 0, 0

    # Combine hardcoded cities + dynamic disaster locations
    static_locations = getattr(settings, 'MONITOR_LOCATIONS', DEFAULT_MONITOR_LOCATIONS)
    disaster_locations = _get_disaster_locations()

    # Deduplicate: keep hardcoded first, then add disaster locations
    seen = set()
    all_locations = []
    for loc in static_locations:
        key = (round(loc['lat'], 1), round(loc['lon'], 1))
        if key not in seen:
            seen.add(key)
            all_locations.append(loc)
    for loc in disaster_locations:
        key = (round(loc['lat'], 1), round(loc['lon'], 1))
        if key not in seen:
            seen.add(key)
            all_locations.append(loc)

    logger.info("Weather API: monitoring %d locations (%d static + %d from disasters)",
                len(all_locations), len(static_locations), len(disaster_locations))

    log = DataFetchLog(source='weather')
    total_fetched = 0
    new_count = 0

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_location_weather, loc): loc for loc in all_locations}
        for future in as_completed(futures):
            try:
                weather, alerts_created = future.result()
                if weather:
                    total_fetched += 1
                    new_count += alerts_created
            except Exception as e:
                logger.warning("Failed to fetch weather for %s: %s", futures[future]['name'], e)

    log.records_fetched = total_fetched
    log.records_new = new_count
    log.save()
    logger.info("Weather API: fetched %d locations, %d new alerts", total_fetched, new_count)
    return new_count, total_fetched


def _fetch_location_weather(location):
    """Fetch weather for a single location. Returns (data_dict, alerts_created_count)."""
    # Use lat/lon for accurate results (works for any location worldwide)
    params = {
        'lat': location['lat'],
        'lon': location['lon'],
        'appid': WEATHER_API_KEY,
        'units': 'metric',
    }

    response = requests.get(
        WEATHER_API_URL,
        params=params,
        timeout=FETCH_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()

    main = data.get('main', {})
    wind = data.get('wind', {})
    rain = data.get('rain', {})
    weather_desc = data.get('weather', [{}])[0]

    temp = main.get('temp')
    humidity = main.get('humidity')
    pressure = main.get('pressure')
    wind_speed = wind.get('speed', 0)
    rainfall = rain.get('1h', 0)

    # Determine severity based on thresholds
    severity, event_type = _classify_weather(temp, humidity, wind_speed, rainfall, pressure)

    if severity is None:
        severity = 'normal'
        event_type = weather_desc.get('main', 'Normal')

    alert_id = _generate_alert_id(location, event_type)

    _, created = WeatherAlert.objects.update_or_create(
        alert_id=alert_id,
        defaults={
            'location_name': location['name'],
            'latitude': location['lat'],
            'longitude': location['lon'],
            'event_type': event_type,
            'severity': severity,
            'description': weather_desc.get('description', ''),
            'temperature': temp,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'pressure': pressure,
            'rainfall': rainfall,
            'start_time': timezone.now(),
            'raw_data': data,
        }
    )

    return data, 1 if created else 0


def _classify_weather(temp, humidity, wind_speed, rainfall, pressure):
    """Classify weather severity. Returns (severity, event_type) or (None, None)."""
    if wind_speed and wind_speed >= EXTREME_THRESHOLDS['wind_speed']:
        if pressure and pressure <= EXTREME_THRESHOLDS['pressure_low']:
            return 'extreme', 'Cyclone/Storm'
        return 'severe', 'High Wind'

    if rainfall and rainfall >= EXTREME_THRESHOLDS['rainfall_1h']:
        return 'severe', 'Heavy Rainfall'

    if temp is not None:
        if temp >= EXTREME_THRESHOLDS['temp_high']:
            return 'severe', 'Extreme Heat'
        if temp <= EXTREME_THRESHOLDS['temp_low']:
            return 'severe', 'Extreme Cold'

    if humidity and humidity >= EXTREME_THRESHOLDS['humidity_high']:
        if rainfall and rainfall > 10:
            return 'moderate', 'Flood Risk'

    if pressure and pressure <= EXTREME_THRESHOLDS['pressure_low']:
        return 'moderate', 'Low Pressure System'

    return None, None


def _generate_alert_id(location, event_type):
    """Generate a deterministic alert ID based on location + event type + date."""
    date_str = timezone.now().strftime('%Y-%m-%d')
    raw = f"{location['name']}_{event_type}_{date_str}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]
