"""
Risk detection engine.

Compares current disaster data against historical baselines
to detect anomalies and compute risk levels per location.
"""
import logging
import math
from datetime import timedelta

from django.db.models import Avg, Max, Count, Q
from django.utils import timezone

from disasters.models import (
    EarthquakeEvent, WeatherAlert,
    LocationBaseline, RiskAssessment,
)

logger = logging.getLogger(__name__)

# Weights for composite risk score
WEIGHTS = {
    'earthquake': 0.55,
    'weather': 0.45,
}

# How far back "current" data extends
RECENT_DAYS = 7

# Anomaly detection: how many standard deviations from baseline triggers high risk
ANOMALY_THRESHOLD = 1.5


def analyze_risks():
    """
    Run full risk analysis across all known locations.
    Deduplicates baselines by rounding to 1 decimal degree to avoid
    hundreds of near-identical entries (e.g. many "Alaska" points).
    Returns list of RiskAssessment objects created.
    """
    baselines = LocationBaseline.objects.all()

    if not baselines.exists():
        logger.warning("No baselines found — run data cleaning first")
        return []

    # Deduplicate: keep only one baseline per rounded (lat, lon) cell
    seen_cells = set()
    unique_baselines = []
    for b in baselines:
        cell = (round(b.latitude, 1), round(b.longitude, 1))
        if cell not in seen_cells:
            seen_cells.add(cell)
            unique_baselines.append(b)

    assessments = []
    now = timezone.now()
    recent_cutoff = now - timedelta(days=RECENT_DAYS)

    for baseline in unique_baselines:
        try:
            assessment = _assess_location(baseline, recent_cutoff)
            if assessment:
                assessments.append(assessment)
        except Exception as e:
            logger.warning("Risk analysis failed for %s: %s", baseline.location_name, e)

    # Rank by risk_score descending
    assessments.sort(key=lambda a: a.risk_score, reverse=True)

    logger.info("Completed risk analysis: %d locations assessed", len(assessments))
    return assessments


def _assess_location(baseline, recent_cutoff):
    """Compute risk for a single location based on all data sources."""
    lat, lon = baseline.latitude, baseline.longitude
    radius = 2.0  # degrees

    # --- Earthquake risk ---
    eq_risk, eq_factors, eq_anomaly = _compute_earthquake_risk(
        lat, lon, radius, recent_cutoff, baseline
    )

    # --- Weather risk ---
    wx_risk, wx_factors, wx_anomaly = _compute_weather_risk(
        lat, lon, radius, recent_cutoff, baseline
    )

    # Build contributing factors list
    contributing = eq_factors + wx_factors

    # Composite score
    risk_score = (
        WEIGHTS['earthquake'] * eq_risk
        + WEIGHTS['weather'] * wx_risk
    )

    # Cross-source amplification: both hazards active
    if eq_risk > 20 and wx_risk > 20:
        risk_score = min(risk_score * 1.15, 100)
        contributing.append('Compound risk: multiple hazard sources active')

    # Weather at disaster site amplification
    if eq_risk > 30 and wx_risk > 30:
        risk_score = min(risk_score + 10, 100)
        contributing.append('Weather conditions worsening disaster impact')

    # Convert to probability (0-1 scale, sigmoid-like capping)
    risk_probability = min(risk_score / 100.0, 0.99)
    risk_probability = round(risk_probability, 2)

    # Determine risk level
    risk_level = _score_to_level(risk_probability)

    # Anomaly detection
    anomaly_detected = eq_anomaly or wx_anomaly
    anomaly_parts = []
    if eq_anomaly:
        anomaly_parts.append("Earthquake activity above historical baseline")
    if wx_anomaly:
        anomaly_parts.append("Weather conditions deviate from historical norms")

    assessment = RiskAssessment.objects.create(
        location_name=baseline.location_name,
        latitude=lat,
        longitude=lon,
        risk_level=risk_level,
        risk_probability=risk_probability,
        risk_score=round(risk_score, 2),
        contributing_factors=contributing,
        earthquake_risk=round(eq_risk, 2),
        weather_risk=round(wx_risk, 2),
        anomaly_detected=anomaly_detected,
        anomaly_details='; '.join(anomaly_parts),
    )

    return assessment


def _compute_earthquake_risk(lat, lon, radius, recent_cutoff, baseline):
    """Compute earthquake risk score (0-100), factors list, and anomaly flag."""
    recent_quakes = EarthquakeEvent.objects.filter(
        latitude__range=(lat - radius, lat + radius),
        longitude__range=(lon - radius, lon + radius),
        event_time__gte=recent_cutoff,
    )

    count = recent_quakes.count()
    stats = recent_quakes.aggregate(
        avg_mag=Avg('magnitude'),
        max_mag=Max('magnitude'),
    )

    if count == 0:
        return 0, [], False

    max_mag = stats['max_mag'] or 0
    avg_mag = stats['avg_mag'] or 0

    # Base score from magnitude (exponential scaling)
    mag_score = min((max_mag / 9.0) ** 2 * 100, 100)

    # Frequency score — compare to baseline
    hist_freq = baseline.earthquake_frequency or 0.1
    # Normalize recent count to monthly rate
    recent_monthly = count * (30 / RECENT_DAYS)
    freq_ratio = recent_monthly / hist_freq if hist_freq > 0 else count

    freq_score = min(freq_ratio * 20, 100)

    # Combined earthquake risk
    risk = 0.6 * mag_score + 0.4 * freq_score

    factors = []
    if max_mag >= 5.0:
        factors.append(f"Significant earthquake M{max_mag:.1f} detected")
    if count > 1:
        factors.append(f"{count} earthquakes in last {RECENT_DAYS} days")

    # Anomaly: current magnitude or frequency significantly exceeds baseline
    anomaly = False
    if baseline.max_earthquake_magnitude > 0:
        if max_mag > baseline.max_earthquake_magnitude * (1 + 1 / ANOMALY_THRESHOLD):
            anomaly = True
    if freq_ratio > ANOMALY_THRESHOLD * 2:
        anomaly = True

    return risk, factors, anomaly


def _compute_weather_risk(lat, lon, radius, recent_cutoff, baseline):
    """Compute weather risk score (0-100) using actual weather measurements."""
    recent_alerts = WeatherAlert.objects.filter(
        latitude__range=(lat - radius, lat + radius),
        longitude__range=(lon - radius, lon + radius),
        fetched_at__gte=recent_cutoff,
    )

    count = recent_alerts.count()
    if count == 0:
        return 0, [], False

    factors = []
    anomaly = False

    # --- Score from severity label (existing logic) ---
    severity_scores = {'extreme': 100, 'severe': 75, 'moderate': 40, 'minor': 15, 'normal': 0}
    max_severity = 0
    for alert in recent_alerts:
        sev_score = severity_scores.get(alert.severity, 10)
        max_severity = max(max_severity, sev_score)

    severity_risk = max_severity

    # --- Score from actual measurements ---
    measurement_risk = 0
    measurement_factors = []

    # Aggregate worst conditions across all alerts at this location
    stats = recent_alerts.aggregate(
        max_rainfall=Max('rainfall'),
        max_wind=Max('wind_speed'),
        max_temp=Max('temperature'),
        min_pressure=Avg('pressure'),  # lower pressure = worse
        max_humidity=Max('humidity'),
    )

    max_rainfall = stats['max_rainfall'] or 0
    max_wind = stats['max_wind'] or 0
    max_temp = stats['max_temp'] or 0
    min_pressure = stats['min_pressure'] or 1013
    max_humidity = stats['max_humidity'] or 0

    # Rainfall risk (0-100): 0mm=0, 50mm+=80, 100mm+=100
    if max_rainfall > 0:
        rain_score = min((max_rainfall / 50.0) * 80, 100)
        measurement_risk = max(measurement_risk, rain_score)
        if max_rainfall >= 50:
            measurement_factors.append(f"Heavy rainfall: {max_rainfall:.1f} mm/h")
        elif max_rainfall >= 20:
            measurement_factors.append(f"Moderate rainfall: {max_rainfall:.1f} mm/h")

    # Wind risk (0-100): 0m/s=0, 20m/s+=80, 35m/s+=100
    if max_wind > 0:
        wind_score = min((max_wind / 20.0) * 80, 100)
        measurement_risk = max(measurement_risk, wind_score)
        if max_wind >= 20:
            measurement_factors.append(f"Dangerous wind: {max_wind:.1f} m/s")
        elif max_wind >= 10:
            measurement_factors.append(f"Strong wind: {max_wind:.1f} m/s")

    # Pressure risk (low pressure = cyclone/storm potential)
    if min_pressure < 1013:
        pressure_deficit = 1013 - min_pressure
        pressure_score = min((pressure_deficit / 30.0) * 80, 100)
        measurement_risk = max(measurement_risk, pressure_score)
        if min_pressure <= 990:
            measurement_factors.append(f"Very low pressure: {min_pressure:.0f} hPa (cyclone risk)")
        elif min_pressure <= 1000:
            measurement_factors.append(f"Low pressure: {min_pressure:.0f} hPa")

    # Temperature extreme risk
    if max_temp >= 45:
        temp_score = min(((max_temp - 40) / 10.0) * 80, 100)
        measurement_risk = max(measurement_risk, temp_score)
        measurement_factors.append(f"Extreme heat: {max_temp:.1f}°C")

    # Compound risk: heavy rain + high wind = worse
    if max_rainfall >= 30 and max_wind >= 15:
        measurement_risk = min(measurement_risk + 20, 100)
        measurement_factors.append("Compound risk: heavy rain + strong wind")

    # Combine: 40% severity label + 60% actual measurements
    risk = 0.4 * severity_risk + 0.6 * measurement_risk

    # Build factors list
    for alert in recent_alerts:
        if alert.severity not in ('normal', 'minor'):
            factors.append(f"{alert.event_type}: {alert.severity} at {alert.location_name}")
    factors.extend(measurement_factors)

    # Anomaly detection against baseline
    if baseline.avg_rainfall is not None and baseline.avg_rainfall > 0:
        if max_rainfall > baseline.avg_rainfall * 2:
            anomaly = True
            risk = min(risk + 15, 100)
            factors.append(f"Rainfall {max_rainfall:.1f}mm exceeds 2x baseline ({baseline.avg_rainfall:.1f}mm)")

    if baseline.avg_wind_speed is not None and baseline.avg_wind_speed > 0:
        if max_wind > baseline.avg_wind_speed * 2:
            anomaly = True
            risk = min(risk + 10, 100)
            factors.append(f"Wind {max_wind:.1f}m/s exceeds 2x baseline ({baseline.avg_wind_speed:.1f}m/s)")

    return risk, factors, anomaly


def _score_to_level(probability):
    """Map risk probability to risk level string."""
    if probability >= 0.80:
        return 'CRITICAL'
    elif probability >= 0.60:
        return 'HIGH'
    elif probability >= 0.35:
        return 'MEDIUM'
    else:
        return 'LOW'


def get_high_risk_locations(limit=10):
    """
    Get the most recent risk assessment batch, ranked by priority.
    Deduplicates by location name — keeps highest risk score per location.
    Returns the JSON-ready output format.
    """
    latest = RiskAssessment.objects.order_by('-assessed_at').first()
    if not latest:
        return {'high_risk_locations': []}

    batch_cutoff = latest.assessed_at - timedelta(minutes=10)
    assessments = RiskAssessment.objects.filter(
        assessed_at__gte=batch_cutoff,
    ).order_by('-risk_probability', '-risk_score')

    # Deduplicate by location name — keep best score per location
    seen_locations = {}
    for assessment in assessments:
        name = assessment.location_name
        if name not in seen_locations or assessment.risk_score > seen_locations[name].risk_score:
            seen_locations[name] = assessment

    # Sort deduplicated results and apply limit
    deduped = sorted(seen_locations.values(), key=lambda a: (-a.risk_score, -a.risk_probability))[:limit]

    results = []
    for rank, assessment in enumerate(deduped, 1):
        results.append({
            'rank': rank,
            'location': assessment.location_name,
            'latitude': assessment.latitude,
            'longitude': assessment.longitude,
            'risk_level': assessment.risk_level,
            'risk_probability': assessment.risk_probability,
            'risk_score': assessment.risk_score,
            'anomaly_detected': assessment.anomaly_detected,
            'anomaly_details': assessment.anomaly_details,
            'contributing_factors': assessment.contributing_factors,
            'earthquake_risk': assessment.earthquake_risk,
            'weather_risk': assessment.weather_risk,
            'assessed_at': assessment.assessed_at.isoformat(),
        })

    return {'high_risk_locations': results}
