from django.contrib import admin
from disasters.models import (
    EarthquakeEvent, WeatherAlert,
    LocationBaseline, RiskAssessment, DataFetchLog,
    GDACSEvent, WildfireEvent,
)


@admin.register(EarthquakeEvent)
class EarthquakeEventAdmin(admin.ModelAdmin):
    list_display = ['title', 'magnitude', 'depth', 'place', 'event_time', 'tsunami']
    list_filter = ['magnitude_type', 'tsunami', 'alert']
    search_fields = ['place', 'title']


@admin.register(WeatherAlert)
class WeatherAlertAdmin(admin.ModelAdmin):
    list_display = ['location_name', 'event_type', 'severity', 'temperature', 'wind_speed', 'start_time']
    list_filter = ['severity', 'event_type']
    search_fields = ['location_name']


@admin.register(LocationBaseline)
class LocationBaselineAdmin(admin.ModelAdmin):
    list_display = ['location_name', 'latitude', 'longitude', 'avg_earthquake_magnitude',
                    'earthquake_frequency', 'total_historical_events', 'last_updated']
    search_fields = ['location_name']


@admin.register(RiskAssessment)
class RiskAssessmentAdmin(admin.ModelAdmin):
    list_display = ['location_name', 'risk_level', 'risk_probability', 'risk_score',
                    'anomaly_detected', 'assessed_at']
    list_filter = ['risk_level', 'anomaly_detected']
    search_fields = ['location_name']


@admin.register(DataFetchLog)
class DataFetchLogAdmin(admin.ModelAdmin):
    list_display = ['source', 'fetched_at', 'records_fetched', 'records_new', 'success']
    list_filter = ['source', 'success']


@admin.register(GDACSEvent)
class GDACSEventAdmin(admin.ModelAdmin):
    list_display = ['title', 'event_type', 'alert_level', 'country', 'event_date', 'severity']
    list_filter = ['event_type', 'alert_level']
    search_fields = ['title', 'country']


@admin.register(WildfireEvent)
class WildfireEventAdmin(admin.ModelAdmin):
    list_display = ['event_id', 'latitude', 'longitude', 'acq_date', 'brightness', 'frp', 'confidence']
    list_filter = ['confidence', 'satellite']
    search_fields = ['country']
