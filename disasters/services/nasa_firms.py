"""Fetches active wildfire/fire data from NASA FIRMS (Fire Information for Resource Management System)."""
import csv
import io
import logging
from datetime import datetime

import requests
from django.conf import settings
from django.utils import timezone

from disasters.models import WildfireEvent, DataFetchLog

logger = logging.getLogger(__name__)

# NASA FIRMS CSV API — requires a free MAP_KEY from https://firms.modaps.eosdis.nasa.gov/api/area/
FIRMS_API_URL = getattr(
    settings, 'NASA_FIRMS_API_URL',
    'https://firms.modaps.eosdis.nasa.gov/api/area/csv'
)
NASA_FIRMS_MAP_KEY = getattr(settings, 'NASA_FIRMS_MAP_KEY', '')
FETCH_TIMEOUT = 60  # FIRMS can be slow for large datasets

# Data source: VIIRS (Visible Infrared Imaging Radiometer Suite) on Suomi-NPP
FIRMS_SOURCE = 'VIIRS_SNPP_NRT'


def fetch_wildfire_events(days=1, area='world'):
    """
    Fetch active fire data from NASA FIRMS.
    days: 1-10 (FIRMS limits to 10 days max)
    area: 'world' or country code like 'IND', 'USA'
    Returns (new_count, total_fetched) tuple.
    """
    if not NASA_FIRMS_MAP_KEY:
        logger.warning("NASA_FIRMS_MAP_KEY not configured — skipping FIRMS fetch")
        log = DataFetchLog(source='nasa_firms', success=False,
                           error_message='NASA_FIRMS_MAP_KEY not configured')
        log.save()
        return 0, 0

    # Clamp days to FIRMS limit
    days = min(max(days, 1), 10)

    url = f"{FIRMS_API_URL}/{NASA_FIRMS_MAP_KEY}/{FIRMS_SOURCE}/{area}/{days}"

    log = DataFetchLog(source='nasa_firms')

    try:
        response = requests.get(url, timeout=FETCH_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("NASA FIRMS fetch failed: %s", e)
        log.success = False
        log.error_message = str(e)
        log.save()
        return 0, 0

    # Parse CSV response
    try:
        reader = csv.DictReader(io.StringIO(response.text))
        rows = list(reader)
    except Exception as e:
        logger.error("NASA FIRMS CSV parse failed: %s", e)
        log.success = False
        log.error_message = f"CSV parse error: {e}"
        log.save()
        return 0, 0

    log.records_fetched = len(rows)
    new_count = 0

    for row in rows:
        try:
            parsed = _parse_fire_row(row)
            if parsed is None:
                continue

            _, created = WildfireEvent.objects.update_or_create(
                event_id=parsed['event_id'],
                defaults=parsed,
            )
            if created:
                new_count += 1
        except Exception as e:
            logger.warning("Failed to process FIRMS fire point: %s", e)
            continue

    log.records_new = new_count
    log.save()
    logger.info("NASA FIRMS: fetched %d fire points, %d new", len(rows), new_count)
    return new_count, len(rows)


def _parse_fire_row(row):
    """Parse a single CSV row from FIRMS into model fields."""
    lat = row.get('latitude')
    lon = row.get('longitude')
    acq_date = row.get('acq_date')

    if not lat or not lon or not acq_date:
        return None

    try:
        latitude = float(lat)
        longitude = float(lon)
    except (ValueError, TypeError):
        return None

    try:
        acq_date_parsed = datetime.strptime(acq_date, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return None

    acq_time = row.get('acq_time', '')
    brightness = _safe_float(row.get('brightness'))
    scan = _safe_float(row.get('scan'))
    track = _safe_float(row.get('track'))
    frp = _safe_float(row.get('frp'))
    confidence = row.get('confidence', '')
    satellite = row.get('satellite', '')
    instrument = row.get('instrument', '')

    # Generate unique ID from coordinates + time
    event_id = f"FIRMS-{latitude:.4f}-{longitude:.4f}-{acq_date}-{acq_time}"

    return {
        'event_id': event_id,
        'latitude': latitude,
        'longitude': longitude,
        'brightness': brightness,
        'scan': scan,
        'track': track,
        'acq_date': acq_date_parsed,
        'acq_time': acq_time,
        'satellite': satellite,
        'instrument': instrument,
        'confidence': confidence,
        'frp': frp,
        'country': row.get('country_id', ''),
        'raw_data': dict(row),
    }


def _safe_float(value):
    """Convert to float, return None on failure."""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
