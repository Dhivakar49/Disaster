"""Management command to automatically fetch data every hour."""
import time
import signal
import sys

from django.core.management.base import BaseCommand

from disasters.services.earthquake_api import fetch_earthquake_events
from disasters.services.weather_api import fetch_weather_data
from disasters.services.gdacs_api import fetch_gdacs_events
from disasters.services.nasa_firms import fetch_wildfire_events
from disasters.services.data_cleaner import clean_and_build_baselines
from disasters.services.risk_analyzer import analyze_risks
from disasters.services.n8n_alert import check_and_alert_earthquakes


class Command(BaseCommand):
    help = 'Automatically fetch disaster data every hour (runs continuously)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--interval', type=int, default=3600,
            help='Fetch interval in seconds (default: 3600 = 1 hour)'
        )
        parser.add_argument(
            '--once', action='store_true',
            help='Run once and exit (useful for Task Scheduler / cron)'
        )

    def handle(self, *args, **options):
        interval = options['interval']
        run_once = options['once']

        # Graceful shutdown on Ctrl+C
        stop = False

        def signal_handler(sig, frame):
            nonlocal stop
            self.stdout.write(self.style.WARNING('\nStopping auto-fetch...'))
            stop = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        if run_once:
            self._run_pipeline()
            return

        self.stdout.write(self.style.SUCCESS(
            f'Auto-fetch started — running every {interval // 60} minutes'
        ))
        self.stdout.write('Press Ctrl+C to stop.\n')

        while not stop:
            self._run_pipeline()

            self.stdout.write(self.style.NOTICE(
                f'\nNext fetch in {interval // 60} minutes. Waiting...\n'
            ))

            # Sleep in small increments so Ctrl+C is responsive
            elapsed = 0
            while elapsed < interval and not stop:
                time.sleep(min(10, interval - elapsed))
                elapsed += 10

        self.stdout.write(self.style.SUCCESS('Auto-fetch stopped.'))

    def _run_pipeline(self):
        """Run a single fetch + analyze cycle."""
        self.stdout.write(self.style.NOTICE('=' * 50))
        self.stdout.write(self.style.NOTICE('Fetching data from all APIs...'))

        try:
            self.stdout.write('  USGS Earthquakes...')
            eq_new, eq_total = fetch_earthquake_events()
            self.stdout.write(f'  -> {eq_total} earthquakes, {eq_new} new')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  Earthquakes failed: {e}'))

        try:
            self.stdout.write('  GDACS disasters...')
            gdacs_new, gdacs_total = fetch_gdacs_events()
            self.stdout.write(f'  -> {gdacs_total} events, {gdacs_new} new')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  GDACS failed: {e}'))

        try:
            self.stdout.write('  NASA FIRMS wildfires...')
            fire_new, fire_total = fetch_wildfire_events()
            self.stdout.write(f'  -> {fire_total} fire points, {fire_new} new')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  NASA FIRMS failed: {e}'))

        try:
            self.stdout.write('  Weather data...')
            wx_new, wx_total = fetch_weather_data()
            self.stdout.write(f'  -> {wx_total} locations, {wx_new} new alerts')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  Weather failed: {e}'))

        try:
            self.stdout.write('  Checking for significant earthquakes (SMS alerts)...')
            alerted = check_and_alert_earthquakes()
            if alerted:
                self.stdout.write(self.style.WARNING(
                    f'  -> SMS alert triggered for {len(alerted)} earthquake(s)'
                ))
            else:
                self.stdout.write('  -> No significant earthquakes requiring SMS alert')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  SMS alert check failed: {e}'))

        try:
            self.stdout.write('  Cleaning & analyzing...')
            clean_and_build_baselines()
            assessments = analyze_risks()
            self.stdout.write(f'  -> {len(assessments)} locations analyzed')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  Analysis failed: {e}'))

        self.stdout.write(self.style.SUCCESS('Fetch cycle complete.'))
