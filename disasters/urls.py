from django.urls import path
from disasters import views

app_name = 'disasters'

urlpatterns = [
    # ─── UI Pages ─────────────────────────────────────
    path('', views.DashboardView.as_view(), name='dashboard'),
    path('events/', views.EventsPageView.as_view(), name='events'),
    path('map/', views.MapPageView.as_view(), name='map_view'),
    path('status/', views.StatusPageView.as_view(), name='status'),

    # ─── API Endpoints (JSON) ─────────────────────────
    path('api/fetch/', views.FetchDataView.as_view(), name='fetch_data'),
    path('api/analyze/', views.AnalyzeView.as_view(), name='analyze'),
    path('api/risks/', views.HighRiskView.as_view(), name='high_risks'),
    path('api/pipeline/', views.FullPipelineView.as_view(), name='full_pipeline'),
    path('api/status/', views.StatusView.as_view(), name='api_status'),
    path('api/weather/', views.WeatherSearchView.as_view(), name='weather_search'),
]
