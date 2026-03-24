"""Fetches earthquake data from USGS Earthquake API."""
import logging
from datetime import datetime, timedelta, timezone as dt_timezone

import requests
from django.conf import settings
from django.utils import timezone

from disasters.models import EarthquakeEvent, DataFetchLog

logger = logging.getLogger(__name__)

EARTHQUAKE_FEEDS = [
    ('significant_month', 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson'),
    ('4.5_month', 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_month.geojson'),
    ('2.5_month', 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.geojson'),
    ('all_month', 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'),
    ('4.5_hour', 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson'),
    ('2.5_hour', 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_hour.geojson'),
    ('all_hour', 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'),
    ('all_day', 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'),
]
FETCH_TIMEOUT = 30


def fetch_earthquake_events(days=30, min_magnitude=2.5):
    """
    Fetch recent earthquakes from USGS feeds (all_hour + all_day + all_month).
    No duplicates — uses update_or_create on event_id.
    Returns (new_count, total_fetched) tuple.
    """
    total_new = 0
    total_fetched = 0

    for feed_name, feed_url in EARTHQUAKE_FEEDS:
        log = DataFetchLog(source='earthquake')

        try:
            response = requests.get(feed_url, timeout=FETCH_TIMEOUT)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error("USGS %s feed fetch failed: %s", feed_name, e)
            log.success = False
            log.error_message = str(e)
            log.save()
            continue

        features = data.get('features', [])
        log.records_fetched = len(features)
        total_fetched += len(features)
        new_count = 0

        for feature in features:
            try:
                parsed = _parse_earthquake(feature)
                if parsed is None:
                    continue

                _, created = EarthquakeEvent.objects.update_or_create(
                    event_id=parsed['event_id'],
                    defaults=parsed,
                )
                if created:
                    new_count += 1
            except Exception as e:
                logger.warning("Failed to process earthquake event: %s", e)
                continue

        log.records_new = new_count
        log.save()
        total_new += new_count
        logger.info("USGS %s: fetched %d events, %d new", feed_name, len(features), new_count)

    return total_new, total_fetched


def _parse_earthquake(feature):
    """Parse a single GeoJSON earthquake feature into model fields."""
    props = feature.get('properties', {})
    geometry = feature.get('geometry', {})
    coords = geometry.get('coordinates', [])

    event_id = feature.get('id')
    if not event_id or len(coords) < 3:
        return None

    longitude, latitude, depth = coords[0], coords[1], coords[2]

    # Convert epoch milliseconds to datetime
    time_ms = props.get('time')
    if time_ms:
        event_time = datetime.fromtimestamp(time_ms / 1000, tz=dt_timezone.utc)
    else:
        return None

    magnitude = props.get('mag')
    if magnitude is None:
        return None

    return {
        'event_id': event_id,
        'title': props.get('title', f"M{magnitude} Earthquake"),
        'latitude': latitude,
        'longitude': longitude,
        'depth': depth,
        'magnitude': float(magnitude),
        'magnitude_type': props.get('magType', ''),
        'place': props.get('place', ''),
        'event_time': event_time,
        'tsunami': bool(props.get('tsunami', 0)),
        'felt': props.get('felt'),
        'significance': props.get('sig', 0),
        'status': props.get('status', ''),
        'alert': props.get('alert'),
        'raw_data': feature,
    }
