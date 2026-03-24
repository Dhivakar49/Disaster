"""Management command to run the full disaster analysis pipeline."""
import json

from django.core.management.base import BaseCommand

from disasters.services.earthquake_api import fetch_earthquake_events
from disasters.services.weather_api import fetch_weather_data
from disasters.services.data_cleaner import clean_and_build_baselines
from disasters.services.risk_analyzer import analyze_risks, get_high_risk_locations
from disasters.services.n8n_alert import check_and_alert_earthquakes


class Command(BaseCommand):
    help = 'Run the full disaster monitoring pipeline: fetch -> clean -> analyze -> output'

    def add_arguments(self, parser):
        parser.add_argument('--days', type=int, default=30, help='Days of historical data to fetch')
        parser.add_argument('--skip-fetch', action='store_true', help='Skip API fetching, analyze existing data')
        parser.add_argument('--limit', type=int, default=10, help='Max risk locations to output')

    def handle(self, *args, **options):
        days = options['days']
        skip_fetch = options['skip_fetch']
        limit = options['limit']

        if not skip_fetch:
            self.stdout.write(self.style.NOTICE('Step 1: Fetching data from APIs...'))

            self.stdout.write('  Fetching earthquake data...')
            eq_new, eq_total = fetch_earthquake_events(days=days)
            self.stdout.write(f'  -> {eq_total} earthquakes fetched, {eq_new} new')

            # Check for significant earthquakes in India (M>=4.5, last 1 hour) and send SMS
            self.stdout.write('  Checking for significant earthquakes in India (M>=4.5)...')
            alerted_quakes = check_and_alert_earthquakes()
            if alerted_quakes:
                self.stdout.write(self.style.WARNING(
                    f'  -> SMS alert triggered for {len(alerted_quakes)} earthquake(s):'
                ))
                for q in alerted_quakes:
                    self.stdout.write(self.style.WARNING(
                        f'     M{q.magnitude} - {q.place}'
                    ))
            else:
                self.stdout.write('  -> No significant earthquakes in India requiring SMS alert')

            self.stdout.write('  Fetching weather data...')
            wx_new, wx_total = fetch_weather_data()
            self.stdout.write(f'  -> {wx_total} locations checked, {wx_new} new alerts')
        else:
            self.stdout.write(self.style.NOTICE('Step 1: Skipping API fetch'))

        self.stdout.write(self.style.NOTICE('\nStep 2: Cleaning data and building baselines...'))
        baselines = clean_and_build_baselines()
        self.stdout.write(f'  -> {baselines} baselines updated')

        self.stdout.write(self.style.NOTICE('\nStep 3: Analyzing risks...'))
        assessments = analyze_risks()
        self.stdout.write(f'  -> {len(assessments)} locations analyzed')

        self.stdout.write(self.style.NOTICE('\nStep 4: Results'))
        output = get_high_risk_locations(limit=limit)
        self.stdout.write(json.dumps(output, indent=2))

        high_risk_count = sum(
            1 for loc in output.get('high_risk_locations', [])
            if loc['risk_level'] in ('HIGH', 'CRITICAL')
        )

        if high_risk_count > 0:
            self.stdout.write(self.style.WARNING(
                f'\n⚠ {high_risk_count} location(s) at HIGH/CRITICAL risk!'
            ))
        else:
            self.stdout.write(self.style.SUCCESS('\n✓ No high-risk locations detected'))
