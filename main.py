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
end_date = start_date

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
            ],
            'limit':100
        },
    },
}

def parse_action(row):
    try: 
        for action in row:
            if action['action_type'] == 'complete_registration':
                return int(action['value'])
    except TypeError:
        return 0
    return 0

token = 'token' 
ad_ac = AdAccount('account_id')
FacebookAdsApi.init(account_idaccount_id access_token=token)
result = ad_ac.get_insights(params=config['age_agg']['params'])
df = pd.DataFrame.from_dict(result)

df['complete_registrations'] = df['actions'].apply(parse_action)
df.drop('date_stop', axis=1, inplace=True)
df.rename({'date_start': 'date_created'}, axis=1, inplace=True)
df['date_created'] = pd.to_datetime(df['date_created'])
df['date_upload'] = date_upload
df[
    [
        'impressions', 'reach', 'unique_clicks', 'unique_inline_link_clicks', 'clicks',
        'cpc', 'cpm', 'cpp', 'ctr', 'frequency', 'spend'
    ]
] = df[
    [
        'impressions', 'reach', 'unique_clicks', 'unique_inline_link_clicks', 'clicks',
        'cpc', 'cpm', 'cpp', 'ctr', 'frequency', 'spend'
    ]
].apply(pd.to_numeric)

agg_data = df.groupby(['account_id', 'date_created', 'ad_id']).agg({'spend': 'sum', 'impressions':'sum', 'clicks':'sum'})
agg_data['cpc'] = agg_data['spend']/agg_data['clicks']

sns.kdeplot(data=agg_data, x="clicks")
sns.kdeplot(data=agg_data, x="cpc")

