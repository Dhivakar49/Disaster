from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('disasters', '0003_alter_datafetchlog_source_gdacsevent_wildfireevent'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='riskassessment',
            name='eonet_risk',
        ),
        migrations.RemoveField(
            model_name='locationbaseline',
            name='eonet_event_frequency',
        ),
        migrations.DeleteModel(
            name='NASAEONETEvent',
        ),
    ]
