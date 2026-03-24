"""Fetches disaster events from GDACS (Global Disaster Alert and Coordination System) API."""
import logging
from datetime import datetime, timedelta, timezone as dt_timezone

import requests
from django.conf import settings
from django.utils import timezone

from disasters.models import GDACSEvent, DataFetchLog

logger = logging.getLogger(__name__)

GDACS_API_URL = getattr(
    settings, 'GDACS_API_URL',
    'https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH'
)
FETCH_TIMEOUT = 30

# Event types to fetch: FL=Flood, TC=Tropical Cyclone, VO=Volcano, DR=Drought
GDACS_EVENT_TYPES = 'FL;TC;VO;DR;WF'
GDACS_ALERT_LEVELS = 'Green;Orange;Red'


def fetch_gdacs_events(days=30):
    """
    Fetch recent disaster events from GDACS API.
    Returns (new_count, total_fetched) tuple.
    """
    to_date = datetime.utcnow()
    from_date = to_date - timedelta(days=days)

    params = {
        'eventlist': GDACS_EVENT_TYPES,
        'fromDate': from_date.strftime('%Y-%m-%d'),
        'toDate': to_date.strftime('%Y-%m-%d'),
        'alertlevel': GDACS_ALERT_LEVELS,
    }

    log = DataFetchLog(source='gdacs')

    try:
        response = requests.get(GDACS_API_URL, params=params, timeout=FETCH_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.error("GDACS API fetch failed: %s", e)
        log.success = False
        log.error_message = str(e)
        log.save()
        return 0, 0

    features = data.get('features', [])
    log.records_fetched = len(features)
    new_count = 0

    for feature in features:
        try:
            parsed = _parse_gdacs_event(feature)
            if parsed is None:
                continue

            _, created = GDACSEvent.objects.update_or_create(
                event_id=parsed['event_id'],
                defaults=parsed,
            )
            if created:
                new_count += 1
        except Exception as e:
            logger.warning("Failed to process GDACS event: %s", e)
            continue

    log.records_new = new_count
    log.save()
    logger.info("GDACS: fetched %d events, %d new", len(features), new_count)
    return new_count, len(features)


def _parse_gdacs_event(feature):
    """Parse a single GDACS GeoJSON feature into model fields."""
    props = feature.get('properties', {})
    geometry = feature.get('geometry', {})

    event_id = props.get('eventid')
    event_type = props.get('eventtype', '')
    if not event_id:
        return None

    # Build a unique event_id combining type and id
    event_id = f"GDACS-{event_type}-{event_id}"

    title = props.get('name', '') or props.get('eventname', '') or f"GDACS {event_type} Event"
    alert_level = props.get('alertlevel', 'Green')

    # Parse coordinates
    latitude, longitude = None, None
    coords = geometry.get('coordinates', [])
    if coords and isinstance(coords, list):
        if isinstance(coords[0], list):
            # Polygon/MultiPoint — use first point
            longitude, latitude = coords[0][0], coords[0][1]
        elif len(coords) >= 2:
            longitude, latitude = coords[0], coords[1]

    # Parse dates
    event_date = _parse_date(props.get('fromdate'))
    end_date = _parse_date(props.get('todate'))

    country = props.get('country', '') or props.get('countryname', '')
    severity = props.get('severitydata', {}).get('severity')
    if severity is not None:
        try:
            severity = float(severity)
        except (ValueError, TypeError):
            severity = None

    population = props.get('populationdata', {}).get('population')
    if population is not None:
        try:
            population = int(float(population))
        except (ValueError, TypeError):
            population = None

    source_url = ''
    url_data = props.get('url')
    if isinstance(url_data, dict):
        source_url = url_data.get('report', '') or url_data.get('details', '')
    elif isinstance(url_data, str):
        source_url = url_data
    description = props.get('htmldescription', '') or props.get('description', '')

    return {
        'event_id': event_id,
        'title': title,
        'event_type': event_type,
        'alert_level': alert_level,
        'latitude': latitude,
        'longitude': longitude,
        'event_date': event_date,
        'end_date': end_date,
        'country': country,
        'severity': severity,
        'population_affected': population,
        'source_url': source_url,
        'description': description[:5000] if description else '',
        'raw_data': feature,
    }


def _parse_date(date_str):
    """Parse GDACS date string into datetime."""
    if not date_str:
        return None
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str[:19], fmt).replace(
                tzinfo=dt_timezone.utc
            )
        except (ValueError, TypeError):
            continue
    return None
