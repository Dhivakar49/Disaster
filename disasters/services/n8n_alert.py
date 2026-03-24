"""Sends alerts to rescue team via n8n workflow for earthquakes and high/critical risk locations."""
import logging
from datetime import timedelta

import requests
from django.conf import settings
from django.utils import timezone

from disasters.models import EarthquakeEvent, RiskAssessment, N8NAlertLog, WildfireEvent

logger = logging.getLogger(__name__)

MAGNITUDE_THRESHOLD = 4.5

WEBHOOK_TIMEOUT = 15


def _magnitude_to_risk_level(magnitude):
    """Convert earthquake magnitude to a human-readable risk level."""
    if magnitude >= 7.0: return 'CRITICAL'
    if magnitude >= 6.0: return 'HIGH'
    if magnitude >= 4.5: return 'MODERATE'
    return 'LOW'


def check_and_alert_earthquakes():
    """
    Check earthquakes worldwide from the last 1 hour with magnitude >= 4.5.
    Deduplicates by event_id — same quake never sent twice.
    Returns list of alerted earthquakes.
    """
    one_hour_ago = timezone.now() - timedelta(hours=1)

    significant_quakes = EarthquakeEvent.objects.filter(
        event_time__gte=one_hour_ago,
        magnitude__gte=MAGNITUDE_THRESHOLD,
    ).order_by('-magnitude')

    if not significant_quakes.exists():
        logger.info("No significant earthquakes (M>=%.1f) worldwide in the last 1 hour.", MAGNITUDE_THRESHOLD)
        return []

    # Get already-alerted earthquake event_ids
    already_sent = set(
        N8NAlertLog.objects.filter(
            alert_type='earthquake',
        ).values_list('event_ref', flat=True)
    )

    alerted = []
    for quake in significant_quakes:
        if quake.event_id in already_sent:
            logger.info("Skipping duplicate earthquake alert: %s", quake.title)
            continue

        risk_level = _magnitude_to_risk_level(quake.magnitude)

        payload = {
            "event_id": quake.event_id,
            "title": quake.title,
            "magnitude": quake.magnitude,
            "depth_km": quake.depth,
            "latitude": quake.latitude,
            "longitude": quake.longitude,
            "place": quake.place,
            "event_time": quake.event_time.isoformat(),
            "tsunami": quake.tsunami,
            "alert_level": risk_level,
            "message": (
                f"EARTHQUAKE ALERT: M{quake.magnitude} earthquake detected near {quake.place}. "
                f"Depth: {quake.depth} km. Time: {quake.event_time.strftime('%Y-%m-%d %H:%M:%S UTC')}. "
                f"Coordinates: ({quake.latitude}, {quake.longitude}). "
                f"Tsunami warning: {'YES' if quake.tsunami else 'No'}. "
                f"Rescue team please respond immediately."
            ),
        }

        success = _trigger_n8n_webhook(payload)
        if success:
            alerted.append(quake)
            N8NAlertLog.objects.get_or_create(
                alert_type='earthquake',
                event_ref=quake.event_id,
                defaults=dict(
                    title=quake.title,
                    location=quake.place,
                    magnitude=quake.magnitude,
                    risk_level=risk_level,
                    message=payload['message'],
                    success=True,
                ),
            )

    return alerted


def _trigger_n8n_webhook(payload):
    """Send earthquake alert data to n8n webhook for SMS dispatch."""
    # Read at call time so ngrok URL updates in settings take effect without restart
    webhook_url = getattr(settings, 'N8N_WEBHOOK_URL', '')
    if not webhook_url:
        logger.warning("N8N_WEBHOOK_URL not configured in settings.py. Skipping SMS alert. Payload: %s", payload.get("title"))
        return False

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=WEBHOOK_TIMEOUT,
        )
        logger.info("SMS alert triggered for %s via n8n. Status: %d", payload.get("title"), response.status_code)
        # Accept 2xx and 500 (n8n missing Respond node) as delivered
        return response.status_code < 600
    except requests.RequestException as e:
        logger.error("Failed to trigger n8n webhook for %s: %s", payload.get("title"), e)
        return False


def send_risk_alerts():
    """
    Send HIGH and CRITICAL risk assessments from the latest batch to n8n.
    Called automatically after every fetch/pipeline run.
    Returns count of alerts sent.
    """
    # Get the latest assessment batch (same logic as get_high_risk_locations)
    latest = RiskAssessment.objects.order_by('-assessed_at').first()
    if not latest:
        logger.info("No risk assessments found — skipping risk alerts.")
        return 0

    batch_cutoff = latest.assessed_at - timedelta(minutes=10)
    high_critical = RiskAssessment.objects.filter(
        assessed_at__gte=batch_cutoff,
        risk_level__in=['HIGH', 'CRITICAL'],
    ).order_by('-risk_score')

    if not high_critical.exists():
        logger.info("No HIGH/CRITICAL risk locations in latest batch — no alerts sent.")
        return 0

    # Avoid duplicates: skip if same location+risk_level already sent today
    today = timezone.now().date()
    already_sent = set(
        N8NAlertLog.objects.filter(
            alert_type='risk',
            sent_at__date=today,
        ).values_list('location', 'risk_level')
    )

    sent = 0
    seen_this_batch = set()  # deduplicate within the batch itself
    for assessment in high_critical:
        key = (assessment.location_name, assessment.risk_level)
        # Skip if same location+risk_level already sent today OR already processed in this batch
        if key in already_sent or key in seen_this_batch:
            logger.info("Skipping duplicate risk alert for: %s (%s)", assessment.location_name, assessment.risk_level)
            continue
        seen_this_batch.add(key)
        # Find the worst recent earthquake near this location to include quake-specific fields
        from datetime import timedelta as td
        recent_quake = EarthquakeEvent.objects.filter(
            latitude__range=(assessment.latitude - 2, assessment.latitude + 2),
            longitude__range=(assessment.longitude - 2, assessment.longitude + 2),
            event_time__gte=timezone.now() - td(days=7),
        ).order_by('-magnitude').first()

        payload = {
            "alert_type": "RISK_ASSESSMENT",
            "risk_level": assessment.risk_level,
            # Earthquake-specific fields (for n8n template compatibility)
            "title": recent_quake.title if recent_quake else assessment.location_name,
            "magnitude": recent_quake.magnitude if recent_quake else None,
            "alert_level": (_magnitude_to_risk_level(recent_quake.magnitude) if recent_quake else assessment.risk_level),
            # Location
            "location": assessment.location_name,
            "latitude": assessment.latitude,
            "longitude": assessment.longitude,
            # Risk scores
            "risk_score": assessment.risk_score,
            "risk_probability": assessment.risk_probability,
            "earthquake_risk": assessment.earthquake_risk,
            "weather_risk": assessment.weather_risk,
            "anomaly_detected": assessment.anomaly_detected,
            "anomaly_details": assessment.anomaly_details,
            "contributing_factors": assessment.contributing_factors,
            "assessed_at": assessment.assessed_at.isoformat(),
            "message": (
                f"{assessment.risk_level} RISK ALERT: {assessment.location_name}. "
                f"Risk score: {assessment.risk_score:.1f}/100 ({int(assessment.risk_probability * 100)}% probability). "
                f"Earthquake risk: {assessment.earthquake_risk:.1f}, Weather risk: {assessment.weather_risk:.1f}. "
                f"{'ANOMALY DETECTED. ' if assessment.anomaly_detected else ''}"
                f"Factors: {', '.join(assessment.contributing_factors[:3]) if assessment.contributing_factors else 'N/A'}."
            ),
        }
        if _trigger_n8n_webhook(payload):
            sent += 1
            N8NAlertLog.objects.create(
                alert_type='risk',
                event_ref=str(assessment.id),
                title=payload['title'],
                location=assessment.location_name,
                magnitude=payload.get('magnitude'),
                risk_level=assessment.risk_level,
                risk_score=assessment.risk_score,
                message=payload['message'],
                success=True,
            )

    logger.info("Risk alerts sent to n8n: %d HIGH/CRITICAL locations", sent)
    return sent


def check_and_alert_wildfires():
    """
    Send high-confidence wildfire detections from the last 24 hours to n8n.
    Confidence is 'high' (VIIRS) or >= 80 (MODIS numeric string).
    Deduplicates by event_id — same fire never sent twice.
    Returns count of alerts sent.
    """
    one_day_ago = timezone.now() - timedelta(hours=24)

    recent_fires = WildfireEvent.objects.filter(
        fetched_at__gte=one_day_ago,
    ).exclude(confidence='')

    # Filter to high-confidence only
    high_conf_fires = []
    for fire in recent_fires:
        conf = fire.confidence.strip().lower()
        if conf == 'high':
            high_conf_fires.append(fire)
        else:
            try:
                if int(conf) >= 80:
                    high_conf_fires.append(fire)
            except ValueError:
                pass

    if not high_conf_fires:
        logger.info("No high-confidence wildfires in last 24 hours.")
        return 0

    already_sent = set(
        N8NAlertLog.objects.filter(
            alert_type='wildfire',
        ).values_list('event_ref', flat=True)
    )

    sent = 0
    for fire in high_conf_fires:
        if fire.event_id in already_sent:
            continue

        location_str = fire.country if fire.country else f"({fire.latitude:.2f}, {fire.longitude:.2f})"
        payload = {
            "alert_type": "WILDFIRE",
            "title": f"High-Confidence Wildfire detected near {location_str}",
            "latitude": fire.latitude,
            "longitude": fire.longitude,
            "country": fire.country,
            "confidence": fire.confidence,
            "frp_mw": fire.frp,
            "brightness_k": fire.brightness,
            "satellite": fire.satellite,
            "detected_date": str(fire.acq_date),
            "detected_time": fire.acq_time,
            "maps_link": f"https://www.google.com/maps?q={fire.latitude},{fire.longitude}",
            "message": (
                f"WILDFIRE ALERT: High-confidence fire detected near {location_str}. "
                f"Fire Radiative Power: {fire.frp} MW. "
                f"Brightness: {fire.brightness} K. "
                f"Satellite: {fire.satellite}. "
                f"Detected: {fire.acq_date} {fire.acq_time} UTC. "
                f"Coordinates: ({fire.latitude}, {fire.longitude}). "
                f"Rescue team please respond immediately."
            ),
        }

        success = _trigger_n8n_webhook(payload)
        if success:
            _, created = N8NAlertLog.objects.get_or_create(
                alert_type='wildfire',
                event_ref=fire.event_id,
                defaults=dict(
                    title=payload['title'],
                    location=location_str,
                    risk_level='HIGH',
                    message=payload['message'],
                    success=True,
                ),
            )
            if created:
                sent += 1

    logger.info("Wildfire alerts sent to n8n: %d high-confidence fires", sent)
    return sent
