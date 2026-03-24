from django.db import models
from django.utils import timezone


class EarthquakeEvent(models.Model):
    """Stores earthquake data from USGS Earthquake API."""
    event_id = models.CharField(max_length=255, unique=True)
    title = models.CharField(max_length=500)
    latitude = models.FloatField()
    longitude = models.FloatField()
    depth = models.FloatField(help_text="Depth in km")
    magnitude = models.FloatField()
    magnitude_type = models.CharField(max_length=10, blank=True)
    place = models.CharField(max_length=500, blank=True)
    event_time = models.DateTimeField()
    tsunami = models.BooleanField(default=False)
    felt = models.IntegerField(null=True, blank=True)
    significance = models.IntegerField(default=0)
    status = models.CharField(max_length=50, blank=True)
    alert = models.CharField(max_length=20, blank=True, null=True)
    raw_data = models.JSONField(default=dict)
    fetched_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-event_time']
        indexes = [
            models.Index(fields=['magnitude']),
            models.Index(fields=['latitude', 'longitude']),
            models.Index(fields=['event_time']),
        ]

    def __str__(self):
        return f"M{self.magnitude} - {self.place}"


class WeatherAlert(models.Model):
    """Stores weather data from Weather API."""
    SEVERITY_CHOICES = [
        ('extreme', 'Extreme'),
        ('severe', 'Severe'),
        ('moderate', 'Moderate'),
        ('minor', 'Minor'),
        ('normal', 'Normal'),
    ]
    alert_id = models.CharField(max_length=255, unique=True)
    location_name = models.CharField(max_length=300)
    latitude = models.FloatField()
    longitude = models.FloatField()
    event_type = models.CharField(max_length=200)
    severity = models.CharField(max_length=20, choices=SEVERITY_CHOICES, default='moderate')
    description = models.TextField(blank=True)
    temperature = models.FloatField(null=True, blank=True)
    humidity = models.FloatField(null=True, blank=True)
    wind_speed = models.FloatField(null=True, blank=True)
    pressure = models.FloatField(null=True, blank=True)
    rainfall = models.FloatField(null=True, blank=True, help_text="Rainfall in mm")
    start_time = models.DateTimeField(null=True, blank=True)
    end_time = models.DateTimeField(null=True, blank=True)
    raw_data = models.JSONField(default=dict)
    fetched_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-start_time']
        indexes = [
            models.Index(fields=['severity']),
            models.Index(fields=['latitude', 'longitude']),
            models.Index(fields=['event_type']),
        ]

    def __str__(self):
        return f"{self.event_type} at {self.location_name} ({self.severity})"


class GDACSEvent(models.Model):
    """Stores disaster events from GDACS API (floods, cyclones, volcanoes, droughts)."""
    EVENT_TYPES = [
        ('FL', 'Flood'),
        ('TC', 'Tropical Cyclone'),
        ('VO', 'Volcano'),
        ('DR', 'Drought'),
        ('WF', 'Wildfire'),
    ]
    ALERT_LEVELS = [
        ('Red', 'Red'),
        ('Orange', 'Orange'),
        ('Green', 'Green'),
    ]
    event_id = models.CharField(max_length=255, unique=True)
    title = models.CharField(max_length=500)
    event_type = models.CharField(max_length=10, choices=EVENT_TYPES)
    alert_level = models.CharField(max_length=20, choices=ALERT_LEVELS, default='Green')
    latitude = models.FloatField(null=True, blank=True)
    longitude = models.FloatField(null=True, blank=True)
    event_date = models.DateTimeField(null=True, blank=True)
    end_date = models.DateTimeField(null=True, blank=True)
    country = models.CharField(max_length=200, blank=True)
    severity = models.FloatField(null=True, blank=True)
    population_affected = models.IntegerField(null=True, blank=True)
    source_url = models.URLField(max_length=1000, blank=True)
    description = models.TextField(blank=True)
    raw_data = models.JSONField(default=dict)
    fetched_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-event_date']
        indexes = [
            models.Index(fields=['event_type']),
            models.Index(fields=['alert_level']),
            models.Index(fields=['latitude', 'longitude']),
            models.Index(fields=['event_date']),
        ]

    def __str__(self):
        return f"{self.title} ({self.get_event_type_display()}) [{self.alert_level}]"


class WildfireEvent(models.Model):
    """Stores active fire/wildfire data from NASA FIRMS."""
    event_id = models.CharField(max_length=255, unique=True)
    latitude = models.FloatField()
    longitude = models.FloatField()
    brightness = models.FloatField(null=True, blank=True, help_text="Brightness temperature (K)")
    scan = models.FloatField(null=True, blank=True)
    track = models.FloatField(null=True, blank=True)
    acq_date = models.DateField()
    acq_time = models.CharField(max_length=10, blank=True)
    satellite = models.CharField(max_length=50, blank=True)
    instrument = models.CharField(max_length=50, blank=True)
    confidence = models.CharField(max_length=20, blank=True)
    frp = models.FloatField(null=True, blank=True, help_text="Fire Radiative Power (MW)")
    country = models.CharField(max_length=200, blank=True)
    raw_data = models.JSONField(default=dict)
    fetched_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-acq_date']
        indexes = [
            models.Index(fields=['latitude', 'longitude']),
            models.Index(fields=['acq_date']),
            models.Index(fields=['confidence']),
        ]

    def __str__(self):
        return f"Fire at ({self.latitude:.2f}, {self.longitude:.2f}) - {self.acq_date}"


class LocationBaseline(models.Model):
    """Historical baseline statistics per location for anomaly detection."""
    location_name = models.CharField(max_length=300)
    latitude = models.FloatField()
    longitude = models.FloatField()
    avg_earthquake_magnitude = models.FloatField(default=0)
    max_earthquake_magnitude = models.FloatField(default=0)
    earthquake_frequency = models.FloatField(default=0, help_text="Average events per month")
    avg_temperature = models.FloatField(null=True, blank=True)
    avg_rainfall = models.FloatField(null=True, blank=True)
    avg_wind_speed = models.FloatField(null=True, blank=True)
    total_historical_events = models.IntegerField(default=0)
    last_updated = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ['location_name', 'latitude', 'longitude']

    def __str__(self):
        return f"Baseline: {self.location_name}"


class RiskAssessment(models.Model):
    """Computed risk assessment for a location."""
    RISK_LEVELS = [
        ('CRITICAL', 'Critical'),
        ('HIGH', 'High'),
        ('MEDIUM', 'Medium'),
        ('LOW', 'Low'),
    ]
    location_name = models.CharField(max_length=300)
    latitude = models.FloatField()
    longitude = models.FloatField()
    risk_level = models.CharField(max_length=20, choices=RISK_LEVELS)
    risk_probability = models.FloatField(help_text="0.0 to 1.0")
    risk_score = models.FloatField(help_text="Composite risk score")
    contributing_factors = models.JSONField(default=list)
    earthquake_risk = models.FloatField(default=0)
    weather_risk = models.FloatField(default=0)
    anomaly_detected = models.BooleanField(default=False)
    anomaly_details = models.TextField(blank=True)
    assessed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-risk_probability', '-risk_score']
        indexes = [
            models.Index(fields=['risk_level']),
            models.Index(fields=['assessed_at']),
        ]

    def __str__(self):
        return f"{self.location_name}: {self.risk_level} ({self.risk_probability:.2f})"


class DataFetchLog(models.Model):
    """Tracks API data fetch history."""
    SOURCE_CHOICES = [
        ('nasa_eonet', 'NASA EONET'),
        ('earthquake', 'USGS Earthquake'),
        ('weather', 'Weather API'),
        ('gdacs', 'GDACS'),
        ('nasa_firms', 'NASA FIRMS'),
    ]
    source = models.CharField(max_length=50)
    fetched_at = models.DateTimeField(auto_now_add=True)
    records_fetched = models.IntegerField(default=0)
    records_new = models.IntegerField(default=0)
    success = models.BooleanField(default=True)
    error_message = models.TextField(blank=True)

    class Meta:
        ordering = ['-fetched_at']

    def __str__(self):
        return f"{self.source} fetch at {self.fetched_at} ({'OK' if self.success else 'FAILED'})"


class N8NAlertLog(models.Model):
    """Tracks every message sent to n8n webhook."""
    ALERT_TYPES = [
        ('earthquake', 'Earthquake'),
        ('risk', 'Risk Assessment'),
        ('wildfire', 'Wildfire'),
    ]
    alert_type = models.CharField(max_length=20, choices=ALERT_TYPES)
    event_ref = models.CharField(max_length=255, blank=True, help_text="Unique event ID or assessment ID")
    title = models.CharField(max_length=500)
    risk_level = models.CharField(max_length=20, blank=True)
    location = models.CharField(max_length=300, blank=True)
    magnitude = models.FloatField(null=True, blank=True)
    risk_score = models.FloatField(null=True, blank=True)
    message = models.TextField(blank=True)
    success = models.BooleanField(default=True)
    sent_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-sent_at']

    def __str__(self):
        return f"{self.alert_type} alert: {self.title} @ {self.sent_at}"



