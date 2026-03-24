"""Data cleaning utilities for disaster data."""
import logging
from datetime import timedelta

from django.db.models import Avg, Max, Count
from django.utils import timezone

from disasters.models import EarthquakeEvent, WeatherAlert, LocationBaseline

logger = logging.getLogger(__name__)

# Radius in degrees for grouping nearby events (~111 km per degree)
LOCATION_RADIUS = 2.0


def clean_and_build_baselines():
    """
    Clean stored data and build/update historical baselines per location.
    Returns number of baselines updated.
    """
    locations = _discover_locations()
    updated = 0

    for loc in locations:
        try:
            _update_baseline(loc)
            updated += 1
        except Exception as e:
            logger.warning("Failed to update baseline for %s: %s", loc['name'], e)

    logger.info("Updated %d location baselines", updated)
    return updated


def _discover_locations():
    """Discover unique locations from all data sources, bucketed to 1-degree grid."""
    locations = {}

    # From earthquake data — group by 1-degree rounded coordinates
    for eq in EarthquakeEvent.objects.values('place', 'latitude', 'longitude'):
        key = _location_key(eq['latitude'], eq['longitude'])
        if key not in locations:
            name = _extract_region_name(eq.get('place', ''))
            locations[key] = {
                'name': name or f"({key[0]:.0f}, {key[1]:.0f})",
                'lat': float(key[0]),
                'lon': float(key[1]),
            }

    # From weather alerts
    for w in WeatherAlert.objects.values('location_name', 'latitude', 'longitude'):
        key = _location_key(w['latitude'], w['longitude'])
        if key not in locations:
            locations[key] = {
                'name': w['location_name'],
                'lat': float(key[0]),
                'lon': float(key[1]),
            }

    return list(locations.values())


def _update_baseline(location):
    """Update or create a baseline for a given location."""
    lat, lon = location['lat'], location['lon']
    radius = LOCATION_RADIUS

    # Earthquake stats in this region
    nearby_quakes = EarthquakeEvent.objects.filter(
        latitude__range=(lat - radius, lat + radius),
        longitude__range=(lon - radius, lon + radius),
    )
    eq_stats = nearby_quakes.aggregate(
        avg_mag=Avg('magnitude'),
        max_mag=Max('magnitude'),
        count=Count('id'),
    )

    # Calculate frequency (events per month)
    oldest_quake = nearby_quakes.order_by('event_time').first()
    if oldest_quake:
        span_days = (timezone.now() - oldest_quake.event_time).days or 1
        eq_frequency = (eq_stats['count'] / span_days) * 30
    else:
        eq_frequency = 0

    # Weather stats
    nearby_weather = WeatherAlert.objects.filter(
        latitude__range=(lat - radius, lat + radius),
        longitude__range=(lon - radius, lon + radius),
    )
    weather_stats = nearby_weather.aggregate(
        avg_temp=Avg('temperature'),
        avg_rain=Avg('rainfall'),
        avg_wind=Avg('wind_speed'),
    )

    total_events = eq_stats['count'] + nearby_weather.count()

    LocationBaseline.objects.update_or_create(
        location_name=location['name'],
        latitude=lat,
        longitude=lon,
        defaults={
            'avg_earthquake_magnitude': eq_stats['avg_mag'] or 0,
            'max_earthquake_magnitude': eq_stats['max_mag'] or 0,
            'earthquake_frequency': eq_frequency,
            'avg_temperature': weather_stats['avg_temp'],
            'avg_rainfall': weather_stats['avg_rain'],
            'avg_wind_speed': weather_stats['avg_wind'],
            'total_historical_events': total_events,
        }
    )


def _location_key(lat, lon):
    """Round coordinates to 2-degree grid to keep location count manageable."""
    return (round(lat / 2) * 2, round(lon / 2) * 2)


def _extract_region_name(place_string):
    """Extract region/country name from USGS place string like '10km NW of City, Country'."""
    if not place_string:
        return ''
    parts = place_string.split(', ')
    if len(parts) >= 2:
        return parts[-1].strip()
    if ' of ' in place_string:
        return place_string.split(' of ')[-1].strip()
    return place_string
