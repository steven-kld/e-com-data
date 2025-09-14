import os, re
from urllib.parse import urlparse, parse_qs, urlunparse
from datetime import timedelta, date
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
import pandas as pd
import numpy as np

from dotenv import load_dotenv
load_dotenv()

GA_EVENTS_TABLE = os.getenv('GA_EVENTS_TABLE')
ORG_TIMEZONE = os.getenv('ORG_TIMEZONE')

# Helpers
def init_google_credentials():
    try:
        required_vars = ["GOOGLE_PROJECT_ID", "GOOGLE_PRIVATE_KEY_ID", "GOOGLE_PRIVATE_KEY", "GOOGLE_CLIENT_EMAIL", "GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_X509_CERT_URL"]
        if not all(os.getenv(v) for v in required_vars):
            print("ERROR: One or more required credentials not found in environment variables.")
            return None
            
        service_account_info = {
            "type": "service_account",
            "project_id": os.getenv("GOOGLE_PROJECT_ID"),
            "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
            "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
            "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_X509_CERT_URL"),
        }
        return Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/cloud-platform'] 
        )
    except Exception as e:
        print(f"ERROR: Failed to initialize credentials. Check your .env file. Error: {e}")
        return None

def extract_gbraid(url):
    try:
        query_params = parse_qs(urlparse(url).query)
        return query_params.get('gbraid', [None])[0]
    except Exception:
        return None

def extract_gad_campaignid(url):
    try:
        query_params = parse_qs(urlparse(url).query)
        return query_params.get('gad_campaignid', [None])[0]
    except Exception:
        return None
    
def load_ads_data(file_path):
    if not os.path.exists(file_path):
        print(f"No such file: {file_path}")
        return None
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Err reading file: {e}")
        return None

def clean_url(url):
    if pd.isna(url):
        return url
    parsed_url = urlparse(url)
    return urlunparse(parsed_url._replace(query=''))

def get_target_page(url):
    if pd.isna(url) or not isinstance(url, str):
        return url
    parsed_url = urlparse(url)
    return re.sub(r'[^a-zA-Z0-9/\-]', '', parsed_url.path)


def query_ga_events_for_google_ads(client, days_ago=7):    
    n_days_ago = (date.today() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

    query_sql = f"""
        SELECT
            event_date,
            event_name,
            user_pseudo_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'gad_campaignid') AS gad_campaignid,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS utm_source,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS utm_medium,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS utm_campaign,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
        FROM
            `{GA_EVENTS_TABLE}`
        WHERE
            event_date >= '{n_days_ago}'
            AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') = 'google'
    """

    query_job = client.query(query_sql)
    return pd.DataFrame([dict(row) for row in query_job.result()])

def ads_raw_report_to_df(raw_path='ads_url_report.csv'):
    ads_df = load_ads_data(raw_path)
    required_columns = ['segments.date', 'campaign.name', 'expanded_landing_page_view.expanded_final_url', 'metrics.impressions', 'metrics.clicks', 'metrics.conversions', 'metrics.cost_micros']
    if not all(col in ads_df.columns for col in required_columns):
        print('missing cols')
    else:
        ads_df['gbraid'] = ads_df['expanded_landing_page_view.expanded_final_url'].apply(extract_gbraid)
        ads_df['gad_campaignid'] = ads_df['expanded_landing_page_view.expanded_final_url'].apply(extract_gad_campaignid)
        ads_df['clean_url'] = ads_df['expanded_landing_page_view.expanded_final_url'].apply(clean_url)
        ads_df['target_page'] = ads_df['clean_url'].apply(get_target_page)
        ads_df['metrics.impressions'] = pd.to_numeric(ads_df['metrics.impressions'], errors='coerce')
        ads_df['metrics.clicks'] = pd.to_numeric(ads_df['metrics.clicks'], errors='coerce')
        ads_df['metrics.conversions'] = pd.to_numeric(ads_df['metrics.conversions'], errors='coerce')
        ads_df['metrics.cost_micros'] = pd.to_numeric(ads_df['metrics.cost_micros'], errors='coerce')
        ads_df['metrics.cost'] = (ads_df['metrics.cost_micros'] / 1_000_000)
        if ads_df is not None and not ads_df.empty and 'campaign.name' in ads_df.columns:
            ads_df['utm_campaign'] = ads_df['campaign.name'].str.lower().str.replace(' ', '_').str.replace(r'(\d{2})\.(\d{2})\.\d{4}', r'\1\2', regex=True).str.replace('.', '')
        
        # 'segments.date' optional group by
        aggregated_df = ads_df.groupby(['target_page', 'gad_campaignid']).agg({
            'metrics.impressions': 'sum',
            'metrics.clicks': 'sum',
            'metrics.conversions': 'sum',
            'metrics.cost': 'sum'
        }).reset_index()

        aggregated_df = aggregated_df.sort_values(by='metrics.impressions', ascending=False)

        try:
            return aggregated_df
        except Exception as e:
            print(f"Error on report saving: {e}")

def ga_raw_events_to_df():
    credentials = init_google_credentials()
    if credentials:
        client = bigquery.Client(credentials=credentials)
        events_df = query_ga_events_for_google_ads(client, days_ago=7)
        if not events_df.empty:
            events_df['clean_link'] = events_df['page_location'].apply(
                lambda x: urlparse(x)._replace(query='', params='').geturl()
            )
            events_df['target_page'] = events_df['clean_link'].apply(get_target_page)
            if events_df is not None and not events_df.empty and 'utm_campaign' in events_df.columns:
                events_df['utm_campaign'] = events_df['utm_campaign'].str.lower().str.replace(' ', '_').str.replace(r'(\d{2})\.(\d{2})\.\d{4}', r'\1\2', regex=True).str.replace('.', '')
        
        return events_df
    
def query_purchase_events(user_pseudos, days_ago=7):
    credentials = init_google_credentials()
    if credentials:
        client = bigquery.Client(credentials=credentials)
        n_days_ago = (date.today() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        pseudo_list_str = ', '.join(f"'{pseudo}'" for pseudo in user_pseudos)
        
        query_sql = f"""
            SELECT
                user_pseudo_id,
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value') AS value,
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'shipping') AS shipping,
                items
            FROM
                `{GA_EVENTS_TABLE}`
            WHERE
                event_date >= '{n_days_ago}'
                AND event_name = 'purchase'
                AND user_pseudo_id IN ({pseudo_list_str})
        """
        query_job = client.query(query_sql)
        purchases = [dict(row) for row in query_job.result()]
        for purchase in purchases:
            item_urls = []
            for item in purchase['items']:
                for param in item['item_params']:
                    if param['key'] == 'item_url':
                        item_urls.append(param['value']['string_value'])
                        break
            purchase['item_urls'] = item_urls
        return purchases

# def get_distinct_df():
#     credentials = init_google_credentials()
#     if credentials:
#         client = bigquery.Client(credentials=credentials)
#         events_df = query_ga_events_for_google_ads(client, days_ago=7)
#         if not events_df.empty:
#             events_df['clean_link'] = events_df['page_location'].apply(
#                 lambda x: urlparse(x)._replace(query='', params='').geturl()
#             )
#             events_df['target_page'] = events_df['clean_link'].apply(get_target_page)
#             events_df['gbraid'] = events_df['page_location'].apply(extract_gbraid)
            
#             distinct_df = events_df.drop_duplicates(subset=['user_pseudo_id', 'utm_campaign', 'page_location']).copy()
            
#             target_pseudos = distinct_df['user_pseudo_id'].tolist()
#             purchases = query_purchase_events(client, target_pseudos, days_ago=7)
#             purchase_map = {}
#             for purchase in purchases:
#                 pseudo = purchase['user_pseudo_id']
#                 if pseudo not in purchase_map:
#                     purchase_map[pseudo] = {
#                         'product_names': [],
#                         'product_urls': [],
#                         'product_prices': [],
#                         'product_quantities': []
#                     }
                
#                 for item in purchase['items']:
#                     item_url = None
#                     for param in item['item_params']:
#                         if param['key'] == 'item_url':
#                             item_url = param['value']['string_value']
#                             break
                    
#                     purchase_map[pseudo]['product_names'].append(item.get('item_name'))
#                     purchase_map[pseudo]['product_urls'].append(item_url)
#                     purchase_map[pseudo]['product_prices'].append(item.get('price'))
#                     purchase_map[pseudo]['product_quantities'].append(item.get('quantity'))
            
#             for col in ['product_names', 'product_urls', 'product_prices', 'product_quantities']:
#                 distinct_df[col] = distinct_df['user_pseudo_id'].map(lambda x: purchase_map.get(x, {}).get(col, []))

#             purchases_df = distinct_df[distinct_df['product_names'].str.len() > 0].copy()

#             # Calculate the total price for each row
#             purchases_df['total_price'] = purchases_df.apply(
#                 lambda row: np.dot(row['product_prices'], row['product_quantities']), axis=1
#             )
            
#             return purchases_df

#         else:
#             print("No events found matching the criteria.")
#             return


def match_gad_target_page_slices_with_ga_events(ads_df, ga_df):
    if 'metrics.clicks' not in ads_df.columns:
        print("ads_df is missing the 'metrics.clicks' column.")
        return pd.DataFrame()

    ads_with_clicks = ads_df[ads_df['metrics.clicks'] > 0].copy()
    matched_events = pd.merge(
        ads_with_clicks, 
        ga_df, 
        on=['gad_campaignid', 'target_page'],
        how='left'
    )
    
    return matched_events
    
if __name__ == '__main__':
    ads_df = ads_raw_report_to_df('ads_url_report.csv')
    ga_df = ga_raw_events_to_df()
    matched_events_df = match_gad_target_page_slices_with_ga_events(ads_df, ga_df)

    if not matched_events_df.empty:
        # Step 1: Get unique user_pseudo_ids from the matched events
        unique_pseudos = matched_events_df['user_pseudo_id'].unique().tolist()
        
        # Step 2: Query for purchases for these unique users
        purchases = query_purchase_events(unique_pseudos, days_ago=7)
        
        # Step 3: Process the purchase data into a DataFrame and calculate total revenue
        if purchases:
            purchases_df = pd.DataFrame(purchases)
            # The 'value' and 'shipping' can be None, so fill with 0
            purchases_df['value'] = purchases_df['value'].fillna(0)
            purchases_df['shipping'] = purchases_df['shipping'].fillna(0)
            purchases_df['net_revenue'] = purchases_df['value'] - purchases_df['shipping']
            
            # Step 4: Merge the total revenue back into the matched_events_df
            final_report = pd.merge(
                matched_events_df,
                purchases_df[['user_pseudo_id', 'net_revenue', 'item_urls']],
                on='user_pseudo_id',
                how='left'
            )
            
            # Finalize the report
            final_report['net_revenue'] = final_report['net_revenue'].fillna(0)
            output_file_name = 'final_ads_ga4_report.xlsx'
            final_report.to_excel(output_file_name, index=False)
    else:
        print("No matching GA events found for Ads clicks.")


