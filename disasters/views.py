import json
import logging
from datetime import timedelta

from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views import View

from disasters.services.earthquake_api import fetch_earthquake_events
from disasters.services.gdacs_api import fetch_gdacs_events
from disasters.services.nasa_firms import fetch_wildfire_events
from disasters.services.data_cleaner import clean_and_build_baselines
from disasters.services.risk_analyzer import analyze_risks, get_high_risk_locations
from disasters.services.n8n_alert import check_and_alert_earthquakes, send_risk_alerts, check_and_alert_wildfires
from disasters.models import (
    DataFetchLog, RiskAssessment,
    EarthquakeEvent, WeatherAlert, GDACSEvent, WildfireEvent, N8NAlertLog,
)

logger = logging.getLogger(__name__)


def _get_risk_color(level):
    return {
        'CRITICAL': '#ef4444', 'HIGH': '#f97316',
        'MEDIUM': '#eab308', 'LOW': '#22c55e',
    }.get(level, '#94a3b8')


def _score_color(val):
    if val >= 60: return '#ef4444'
    if val >= 30: return '#eab308'
    if val > 0: return '#3b82f6'
    return '#94a3b8'


def _build_risk_list():
    output = get_high_risk_locations(limit=500)
    risks = output.get('high_risk_locations', [])
    for r in risks:
        r['risk_color'] = _get_risk_color(r['risk_level'])
        r['probability_pct'] = int(r['risk_probability'] * 100)
        r['eq_color'] = _score_color(r['earthquake_risk'])
        r['wx_color'] = _score_color(r['weather_risk'])
    return risks


def _build_stats(risks):
    return {
        'critical_count': sum(1 for r in risks if r['risk_level'] == 'CRITICAL'),
        'high_count': sum(1 for r in risks if r['risk_level'] == 'HIGH'),
        'medium_count': sum(1 for r in risks if r['risk_level'] == 'MEDIUM'),
        'low_count': sum(1 for r in risks if r['risk_level'] == 'LOW'),
        'total_locations': len(risks),
        'anomaly_count': sum(1 for r in risks if r.get('anomaly_detected')),
        'total_earthquakes': EarthquakeEvent.objects.count(),
        'total_weather': WeatherAlert.objects.count(),
        'total_gdacs': GDACSEvent.objects.count(),
    }


class DashboardView(View):
    def get(self, request):
        from datetime import timedelta
        risks = _build_risk_list()
        stats = _build_stats(risks)
        critical_risks = [r for r in risks if r['risk_level'] == 'CRITICAL']
        high_risks = [r for r in risks if r['risk_level'] == 'HIGH']

        # Recent events for map — last 24 hours only
        now = timezone.now()
        one_day_ago = now - timedelta(hours=24)

        recent_quakes = list(EarthquakeEvent.objects.filter(
            event_time__gte=one_day_ago
        ).order_by('-magnitude').values(
            'latitude', 'longitude', 'magnitude', 'place', 'event_time', 'depth', 'tsunami'
        )[:200])
        for q in recent_quakes:
            q['event_time'] = q['event_time'].strftime('%b %d %H:%M UTC') if q['event_time'] else ''

        recent_gdacs = list(GDACSEvent.objects.filter(
            event_date__gte=one_day_ago
        ).exclude(latitude__isnull=True).order_by('-event_date').values(
            'latitude', 'longitude', 'title', 'event_type', 'alert_level', 'country', 'event_date'
        )[:100])
        for g in recent_gdacs:
            g['event_date'] = g['event_date'].strftime('%b %d %H:%M UTC') if g['event_date'] else ''

        recent_wildfires = list(WildfireEvent.objects.filter(
            fetched_at__gte=one_day_ago
        ).order_by('-frp').values(
            'latitude', 'longitude', 'frp', 'confidence', 'acq_date', 'country'
        )[:100])
        for w in recent_wildfires:
            w['acq_date'] = str(w['acq_date']) if w['acq_date'] else ''

        return render(request, 'disasters/dashboard.html', {
            'active_page': 'dashboard',
            'risks': risks,
            'risks_json': json.dumps(risks),
            'stats': stats,
            'stats_json': json.dumps(stats),
            'critical_risks': critical_risks,
            'high_risks': high_risks,
            'has_alerts': len(critical_risks) + len(high_risks) > 0,
            'recent_quakes_json': json.dumps(recent_quakes),
            'recent_gdacs_json': json.dumps(recent_gdacs),
            'recent_wildfires_json': json.dumps(recent_wildfires),
        })


class EventsPageView(View):
    def get(self, request):
        now = timezone.now()
        one_hour_ago = now - timedelta(hours=1)
        one_day_ago = now - timedelta(days=1)
        one_month_ago = now - timedelta(days=30)

        # Earthquakes
        earthquakes_hour = list(EarthquakeEvent.objects.filter(event_time__gte=one_hour_ago).order_by('-event_time')[:100])
        earthquakes_day = list(EarthquakeEvent.objects.filter(event_time__gte=one_day_ago, event_time__lt=one_hour_ago).order_by('-event_time')[:200])
        earthquakes_month = list(EarthquakeEvent.objects.filter(event_time__gte=one_month_ago, event_time__lt=one_day_ago).order_by('-event_time')[:500])
        earthquakes_india = list(EarthquakeEvent.objects.filter(
            latitude__gte=6, latitude__lte=37,
            longitude__gte=68, longitude__lte=98,
        ).order_by('-event_time')[:200])

        # Floods
        gdacs_floods = list(GDACSEvent.objects.filter(event_type='FL').order_by('-event_date')[:200])
        heavy_rain_alerts = list(WeatherAlert.objects.filter(rainfall__gte=50).order_by('-fetched_at')[:100])

        # Cyclones / Storms
        gdacs_cyclones = list(GDACSEvent.objects.filter(event_type='TC').order_by('-event_date')[:200])
        storm_alerts = list(WeatherAlert.objects.filter(event_type__icontains='storm').order_by('-fetched_at')[:100])

        # Volcanoes
        volcano_events = list(GDACSEvent.objects.filter(event_type='VO').order_by('-event_date')[:200])

        # Wildfires
        wildfire_firms = list(WildfireEvent.objects.all().order_by('-acq_date')[:200])
        gdacs_wildfires = list(GDACSEvent.objects.filter(event_type='WF').order_by('-event_date')[:100])

        # Droughts
        drought_events = list(GDACSEvent.objects.filter(event_type='DR').order_by('-event_date')[:200])

        # Weather Alerts
        weather_alerts = list(WeatherAlert.objects.all().order_by('-fetched_at')[:200])

        return render(request, 'disasters/events.html', {
            'active_page': 'events',
            'earthquakes_hour': earthquakes_hour,
            'earthquakes_day': earthquakes_day,
            'earthquakes_month': earthquakes_month,
            'earthquakes_india': earthquakes_india,
            'gdacs_floods': gdacs_floods,
            'heavy_rain_alerts': heavy_rain_alerts,
            'flood_total': len(gdacs_floods) + len(heavy_rain_alerts),
            'gdacs_cyclones': gdacs_cyclones,
            'storm_alerts': storm_alerts,
            'cyclone_total': len(gdacs_cyclones) + len(storm_alerts),
            'volcano_events': volcano_events,
            'volcano_total': len(volcano_events),
            'wildfire_firms': wildfire_firms,
            'gdacs_wildfires': gdacs_wildfires,
            'wildfire_total': len(wildfire_firms) + len(gdacs_wildfires),
            'drought_events': drought_events,
            'drought_total': len(drought_events),
            'weather_alerts': weather_alerts,
            'weather_total': len(weather_alerts),
        })


class MapPageView(View):
    def get(self, request):
        risks = _build_risk_list()
        critical_risks = [r for r in risks if r['risk_level'] == 'CRITICAL']
        high_risks = [r for r in risks if r['risk_level'] == 'HIGH']
        now = timezone.now()
        one_day_ago = now - timedelta(hours=24)

        earthquakes = list(EarthquakeEvent.objects.filter(
            event_time__gte=one_day_ago
        ).order_by('-magnitude').values(
            'latitude', 'longitude', 'magnitude', 'place', 'event_time', 'depth', 'tsunami'
        )[:300])
        for eq in earthquakes:
            eq['event_time'] = eq['event_time'].strftime('%b %d, %Y %H:%M UTC') if eq['event_time'] else ''

        gdacs_events = list(GDACSEvent.objects.filter(
            event_date__gte=one_day_ago
        ).exclude(latitude__isnull=True).order_by('-event_date').values(
            'latitude', 'longitude', 'title', 'event_type', 'alert_level', 'country', 'event_date'
        )[:200])
        for ev in gdacs_events:
            ev['event_date'] = ev['event_date'].strftime('%b %d, %Y %H:%M UTC') if ev['event_date'] else ''

        wildfire_events = list(WildfireEvent.objects.filter(
            fetched_at__gte=one_day_ago
        ).order_by('-frp').values(
            'latitude', 'longitude', 'frp', 'confidence', 'acq_date', 'country'
        )[:200])
        for wf in wildfire_events:
            wf['acq_date'] = str(wf['acq_date']) if wf['acq_date'] else ''

        return render(request, 'disasters/map.html', {
            'active_page': 'map',
            'risks_json': json.dumps(risks),
            'earthquakes_json': json.dumps(earthquakes),
            'gdacs_json': json.dumps(gdacs_events),
            'wildfires_json': json.dumps(wildfire_events),
            'critical_risks': critical_risks,
            'high_risks': high_risks,
            'has_alerts': len(critical_risks) + len(high_risks) > 0,
            'eq_count': len(earthquakes),
            'gdacs_count': len(gdacs_events),
            'fire_count': len(wildfire_events),
        })


class FetchDataView(View):
    def get(self, request):
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _run():
            tasks = {
                'earthquake': fetch_earthquake_events,
                'gdacs': fetch_gdacs_events,
                'nasa_firms': fetch_wildfire_events,
            }
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(fn): name for name, fn in tasks.items()}
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error("Fetch failed for %s: %s", name, e)
            try:
                check_and_alert_earthquakes()
                send_risk_alerts()
                check_and_alert_wildfires()
            except Exception as e:
                logger.error("Alert error: %s", e)

        threading.Thread(target=_run, daemon=True).start()
        return JsonResponse({'status': 'started', 'message': 'Fetching data in background. Refresh in 30 seconds.'})


class AnalyzeView(View):
    def get(self, request):
        baselines_updated = clean_and_build_baselines()
        assessments = analyze_risks()
        output = get_high_risk_locations(limit=20)
        return JsonResponse({'status': 'success', 'baselines_updated': baselines_updated, 'locations_analyzed': len(assessments), **output})


class HighRiskView(View):
    def get(self, request):
        limit = int(request.GET.get('limit', 10))
        return JsonResponse(get_high_risk_locations(limit=limit))


class FullPipelineView(View):
    def get(self, request):
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _run():
            # Step 1: Fetch in parallel
            tasks = {
                'earthquake': fetch_earthquake_events,
                'gdacs': fetch_gdacs_events,
                'nasa_firms': fetch_wildfire_events,
            }
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(fn): name for name, fn in tasks.items()}
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error("Fetch failed for %s: %s", name, e)

            # Step 2: Earthquake alerts
            try:
                check_and_alert_earthquakes()
            except Exception as e:
                logger.error("Earthquake alert error: %s", e)

            # Step 3: Clean + analyze
            try:
                clean_and_build_baselines()
                analyze_risks()
            except Exception as e:
                logger.error("Analysis error: %s", e)

            # Step 4: n8n alerts
            try:
                send_risk_alerts()
                check_and_alert_wildfires()
            except Exception as e:
                logger.error("n8n alert error: %s", e)

        threading.Thread(target=_run, daemon=True).start()
        return JsonResponse({
            'status': 'started',
            'message': 'Pipeline running in background. Refresh in 60 seconds.',
        })


class StatusView(View):
    def get(self, request):
        latest_fetches = {}
        for source in ['earthquake', 'weather', 'gdacs', 'nasa_firms']:
            log = DataFetchLog.objects.filter(source=source).first()
            latest_fetches[source] = {
                'last_fetch': log.fetched_at.isoformat(),
                'records_fetched': log.records_fetched,
                'records_new': log.records_new,
                'success': log.success,
            } if log else None
        return JsonResponse({'status': 'operational', 'latest_fetches': latest_fetches,
                             'total_risk_assessments': RiskAssessment.objects.count()})


class StatusPageView(View):
    def get(self, request):
        logs = N8NAlertLog.objects.all()[:50]
        return render(request, 'disasters/status.html', {'active_page': 'status', 'n8n_logs': logs})


class WeatherSearchView(View):
    """On-demand weather lookup for a user-specified place name."""
    def get(self, request):
        place = request.GET.get('place', '').strip()
        if not place:
            return JsonResponse({'error': 'No place provided'}, status=400)

        from django.conf import settings
        import requests as req

        api_key = getattr(settings, 'WEATHER_API_KEY', '')
        if not api_key:
            return JsonResponse({'error': 'Weather API key not configured'}, status=500)

        try:
            resp = req.get(
                'https://api.openweathermap.org/data/2.5/weather',
                params={'q': place, 'appid': api_key, 'units': 'metric'},
                timeout=10,
            )
            if resp.status_code == 404:
                return JsonResponse({'error': f'Place "{place}" not found'}, status=404)
            resp.raise_for_status()
            d = resp.json()

            main = d.get('main', {})
            wind = d.get('wind', {})
            rain = d.get('rain', {})
            weather = d.get('weather', [{}])[0]
            sys = d.get('sys', {})

            return JsonResponse({
                'place': d.get('name', place),
                'country': sys.get('country', ''),
                'latitude': d.get('coord', {}).get('lat'),
                'longitude': d.get('coord', {}).get('lon'),
                'temperature': main.get('temp'),
                'feels_like': main.get('feels_like'),
                'temp_min': main.get('temp_min'),
                'temp_max': main.get('temp_max'),
                'humidity': main.get('humidity'),
                'pressure': main.get('pressure'),
                'wind_speed': wind.get('speed'),
                'wind_deg': wind.get('deg'),
                'rainfall_1h': rain.get('1h', 0),
                'description': weather.get('description', ''),
                'icon': weather.get('icon', ''),
                'visibility': d.get('visibility'),
                'clouds': d.get('clouds', {}).get('all'),
            })
        except req.exceptions.RequestException as e:
            logger.error("Weather search failed for %s: %s", place, e)
            return JsonResponse({'error': 'Weather service unavailable'}, status=503)



