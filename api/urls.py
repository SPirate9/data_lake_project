from django.urls import path
from rest_framework.authtoken.views import obtain_auth_token
from . import views

urlpatterns = [
    path('api-token-auth/', obtain_auth_token),
    path('permissions/add/', views.add_permission),
    path('permissions/remove/', views.remove_permission),
    path('data/', views.get_data_lake),
    path('metrics/last5min/', views.money_spent_last_5min),
    path('metrics/total_per_user/', views.total_spent_per_user_and_type),
    path('metrics/top_products/', views.top_x_products),
    path('data/version/', views.get_data_version),
    path('logs/access/', views.access_logs),
    path('resources/', views.list_resources),
    path('full_text_search/', views.full_text_search),
    path('ml/train/', views.train_ml_model),
    path('repush/transaction/', views.repush_transaction),
    path('repush/all_transactions/', views.repush_all_transactions),
]