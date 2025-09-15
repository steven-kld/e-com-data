import os, re, json
from urllib.parse import urlparse, parse_qs, urlunparse
from datetime import timedelta, date
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
import pandas as pd
from get_shopify_sessions import get_orders_data, get_products_by_ids

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
    path = parsed_url.path

    cleaned_path = re.sub(r'(%[0-9a-fA-F]{2,4})|(\\u[0-9a-fA-F]{4})', '', path)
    final_path = re.sub(r'[^a-zA-Z0-9/\-]', '', cleaned_path)
    return final_path

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

def parse_landing_site_url(url_string):
    parsed_url = urlparse(url_string)
    query_params = parse_qs(parsed_url.query)

    utm_source = query_params.get('utm_source', [None])[0]
    utm_medium = query_params.get('utm_medium', [None])[0]
    utm_campaign = query_params.get('utm_campaign', [None])[0]
    utm_content = query_params.get('utm_content', [None])[0]
    utm_term = query_params.get('utm_term', [None])[0]
    gad_campaignid = query_params.get('gad_campaignid', [None])[0]
    gbraid = query_params.get('gbraid', [None])[0]
    gclid = query_params.get('gclid', [None])[0]

    target_page = parsed_url.path
    final_target_page = re.sub(r'[^a-zA-Z0-9/\-]', '', target_page)

    return {
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": utm_campaign,
        "utm_content": utm_content,
        "utm_term": utm_term,
        "gad_campaignid": gad_campaignid,
        "gbraid": gbraid,
        "gclid": gclid,
        "path": target_page,
        "target_page": final_target_page
    }


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


def match_and_aggregate_revenue(ads_df, orders):
    if 'target_page' not in ads_df.columns or 'gad_campaignid' not in ads_df.columns:
        print("ads_df is missing required columns. Cannot proceed.")
        return ads_df
        
    ads_df['total_revenue'] = 0.0
    ads_df['total_purchases'] = 0.0
    ads_df['items'] = [[] for _ in range(len(ads_df))]
    ads_df['handles'] = [[] for _ in range(len(ads_df))]
    
    lookup_dict = {
        (row['target_page'], str(row['gad_campaignid'])): index 
        for index, row in ads_df.iterrows()
    }
    
    for order in orders:
        order_target_page = order.get('target_page')
        order_gad_campaignid = order.get('gad_campaignid')
        # Check if the keys exist and are not None before creating the lookup key
        if order_target_page and order_gad_campaignid:
            match_key = (order_target_page, str(order_gad_campaignid))
            
            if match_key in lookup_dict:
                match_index = lookup_dict[match_key]
                
                # Use .loc to safely update the DataFrame row by index
                ads_df.loc[match_index, 'total_revenue'] += order.get('net_revenue', 0.0)
                ads_df.loc[match_index, 'total_purchases'] += 1

                # Get the existing list of items and extend it
                existing_items = ads_df.loc[match_index, 'items']
                new_item_ids = [item.get('item_id') for item in order['products']]
                existing_items.extend(new_item_ids)
                existing_handles = ads_df.loc[match_index, 'handles']
                new_item_handles = [item.get('handle') for item in order['products']]
                existing_handles.extend(new_item_handles)
                
    return ads_df

def add_is_waste_column(df):
    if df.empty or 'metrics.cost' not in df.columns or 'total_purchases' not in df.columns:
        raise ValueError("DataFrame is empty or missing required columns.")
    
    converting_campaigns = df[df['total_purchases'] > 0].copy()
    total_cost_conv = converting_campaigns['metrics.cost'].sum()
    total_conversions_conv = converting_campaigns['total_purchases'].sum()
    acpc = total_cost_conv / total_conversions_conv if total_conversions_conv > 0 else 0
    threshold = acpc * 1.5
    print(threshold)
    df['cpa'] = df.apply(
        lambda row: row['metrics.cost'] / row['total_purchases'] if row['total_purchases'] > 0 else row['metrics.cost'],
        axis=1
    )

    df['is_waste'] = df['cpa'] > threshold

    return df



if __name__ == '__main__':
    from_date = date(2025, 8, 16)
    orders_data = get_orders_data(os.getenv("SHOPIFY_API_KEY"), os.getenv("SHOPIFY_DOMAIN"), from_date.isoformat())

    orders = []
    products_dict = {}
    if orders_data:
        for order in orders_data:
            params = parse_landing_site_url(order.get('landingSite'))
            params['products'] = order['products']
            for product in order['products']:
                products_dict[product['item_id']] = ''
            params['net_revenue'] = sum(float(item.get('price', 0)) * float(item.get('quantity', 0)) for item in order['products'])
            if params.get('utm_source') == 'google':
                orders.append(params)

    products_with_handles = get_products_by_ids(products_dict)
    for order in orders:
        for product in order['products']:
            item_id = product.get('item_id')
            if item_id:
                product['handle'] = products_with_handles.get(item_id)

    ads_df = ads_raw_report_to_df('ads_url_report.csv')
    if not ads_df.empty and orders:
        final_report_df = match_and_aggregate_revenue(ads_df, orders)
        add_is_waste_column(final_report_df)
        print("\n--- Final Report with Revenue and Items ---")
        print(final_report_df)
        
        output_file_name = 'final_ads_ga4_report.xlsx'
        final_report_df.to_excel(output_file_name, index=False)
        print(f"Report saved: {output_file_name}")

    else:
        print("No Ads data or Google-sourced orders found.")
