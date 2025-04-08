import pandas as pd
import requests
import logging
import time
import os
import google.auth
from access_data import creds
from datetime import datetime, timedelta
from google.cloud import bigquery
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.oauth2 import service_account


end_date = datetime.today().strftime('%Y-%m-%d')
start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
date_upload = pd.to_datetime(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))

config = {
    'age_agg': {
        'params': {
            'level': AdsInsights.Level.ad,
            'fields': [
                'account_name',
                'account_id',
                'adset_id',
                'adset_name',
                'campaign_name',
                'campaign_id',
                'ad_name',
                'ad_id',
                'impressions',
                'spend',
                'clicks',
                'unique_clicks',
                'unique_inline_link_clicks',
                'reach',
                'quality_ranking',
                'conversions',
                'cpc',
                'cpm',
                'cpp',
                'ctr',
                'actions',
                'engagement_rate_ranking',
                'frequency',
                'optimization_goal',
                'account_currency', 
                'place_page_name'
            ],
            'time_range': {'since':start_date,'until':end_date},
            'time_increment': 1,
            'breakdowns': [
                'age', 
                'gender' 
            ]
        },
    },
}


creds = list(creds.items())
ad_ac = AdAccount(creds[0][0])
result = ad_ac.get_insights(params=config['age_agg']['params'])
df = pd.DataFrame.from_dict(result)




