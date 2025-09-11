import os, json
from datetime import datetime
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from db import run_many_query

GA_EVENTS_TABLE = os.getenv('GA_EVENTS_TABLE')

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
            # Add BigQuery scopes here
            scopes=['https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/cloud-platform'] 
        )
    except Exception as e:
        print(f"ERROR: Failed to initialize credentials. Check your .env file. Error: {e}")
        return None
    
def query_last_ga_events():    
    client = bigquery.Client(credentials=init_google_credentials())
    query_sql = f"""
        # Category 1: Purchase-related events
        (
            SELECT
                event_date,
                event_timestamp,
                event_name,
                user_pseudo_id,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS utm_source,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS utm_medium,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS utm_campaign,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term') AS utm_term,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content') AS utm_content,
                event_params,
                ecommerce,
                items
            FROM
                `{GA_EVENTS_TABLE}`
            WHERE
                event_name IN ('purchase', 'form_submit', 'add_payment_info', 'add_shipping_info', 'begin_checkout', 'add_to_cart')
        )

        UNION ALL

        # Category 2: Events with UTMs
        (
            SELECT
                event_date,
                event_timestamp,
                event_name,
                user_pseudo_id,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS utm_source,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS utm_medium,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS utm_campaign,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term') AS utm_term,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content') AS utm_content,
                event_params,
                ecommerce,
                items
            FROM
                `{GA_EVENTS_TABLE}`
            WHERE
                EXISTS (SELECT 1 FROM UNNEST(event_params) AS param WHERE param.key IN ('source', 'medium', 'campaign', 'term', 'content'))
        )

        UNION ALL

        # Category 3: Earliest event for users with no UTMs or purchase-related events
        (
            WITH excluded_users AS (
                SELECT DISTINCT user_pseudo_id
                FROM `{GA_EVENTS_TABLE}`
                WHERE 
                    event_name IN ('purchase', 'form_submit', 'add_payment_info', 'add_shipping_info', 'begin_checkout', 'add_to_cart')
                    OR EXISTS (SELECT 1 FROM UNNEST(event_params) AS param WHERE param.key LIKE 'utm_%')
            ),
            earliest_events AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS rn
                FROM `{GA_EVENTS_TABLE}`
                WHERE
                    user_pseudo_id NOT IN (SELECT user_pseudo_id FROM excluded_users)
            )
            SELECT
                event_date,
                event_timestamp,
                event_name,
                user_pseudo_id,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS utm_source,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS utm_medium,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS utm_campaign,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term') AS utm_term,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content') AS utm_content,
                event_params,
                ecommerce,
                items
            FROM
                earliest_events
            WHERE
                rn = 1
        )

        ORDER BY event_timestamp DESC;
    """

    query_job = client.query(query_sql)
    results = query_job.result()
    
    processed_rows = []
    for row in results:
        row_dict = dict(row.items())
        
        processed_dict = {}
        processed_dict['event_timestamp'] = row_dict.get('event_timestamp')
        processed_dict['event_name'] = row_dict.get('event_name')
        processed_dict['event_date'] = row_dict.get('event_date')
        processed_dict['user_pseudo_id'] = row_dict.get('user_pseudo_id')
        processed_dict['event_params'] = {}

        event_params_flat = {}
        for param in row_dict.get('event_params', []):
            key = param.get('key')
            value_obj = param.get('value', {})
            if key:
                if value_obj.get('string_value') is not None:
                    event_params_flat[key] = value_obj['string_value']
                elif value_obj.get('int_value') is not None:
                    event_params_flat[key] = value_obj['int_value']
                elif value_obj.get('float_value') is not None:
                    event_params_flat[key] = value_obj['float_value']
                elif value_obj.get('double_value') is not None:
                    event_params_flat[key] = value_obj['double_value']

        processed_dict['utm_source'] = event_params_flat.get('source')
        processed_dict['utm_campaign'] = event_params_flat.get('campaign')
        processed_dict['utm_medium'] = event_params_flat.get('medium')
        processed_dict['utm_term'] = event_params_flat.get('term')

        if row_dict['event_name'] in ['purchase', 'form_submit', 'add_payment_info', 'add_shipping_info', 'begin_checkout', 'add_to_cart']:
            try:
                processed_dict['event_params']['order_total'] = row_dict['ecommerce']['purchase_revenue']
                processed_dict['event_params']['shipping_value'] = row_dict['ecommerce']['shipping_value']
            except:
                processed_dict['event_params']['order_total'] = event_params_flat.get('value')
                processed_dict['event_params']['shipping_value'] = 0
            
            products_list = []
            for product in row_dict['items']:
                price = product.get('price') or 0
                quantity = product.get('quantity') or 0
                try:
                    item_id = int(product.get('item_id', 0))
                except:
                    item_id = 0
                products_list.append({
                    "item_id": item_id,
                    "price": float(price),
                    "quantity": int(quantity)
                })

            order_total = processed_dict['event_params'].get('order_total') or event_params_flat.get('value') or 0
            shipping_value = processed_dict['event_params'].get('shipping_value') or 0

            products_sum = sum(p['price'] * p['quantity'] for p in products_list)

            if order_total == 0 or shipping_value == 0:
                processed_dict['event_params']['order_total'] = order_total
                processed_dict['event_params']['shipping_value'] = order_total - products_sum

            processed_dict['event_params']['products'] = products_list

        processed_rows.append(processed_dict)
    
    return processed_rows

def insert_ga_events(events_list):
    if not events_list:
        print("No events to insert.")
        return
        
    query_sql = """
        INSERT INTO ga_events (
            ga_user_pseudo_id,
            event_name,
            event_timestamp,
            event_timestamp_numeric,
            utm_source,
            utm_campaign,
            utm_medium,
            event_params
        )
        VALUES %s
        ON CONFLICT (ga_user_pseudo_id, event_timestamp) DO NOTHING
    """
    
    data_to_insert = []
    for event in events_list:
        # Check for required fields before processing
        if not event.get('user_pseudo_id') or not event.get('event_timestamp'):
            print(f"Skipping event due to missing pseudo ID or timestamp: {event}")
            continue
            
        # Convert BigQuery's microsecond timestamp (BIGINT) to a Python datetime object
        event_timestamp_bigint = event.get('event_timestamp')
        try:
            event_dt = datetime.fromtimestamp(event_timestamp_bigint / 1_000_000)
            event_dt_str = event_dt.strftime('%Y-%m-%d %H:%M:%S.%f %z')
        except (ValueError, TypeError) as e:
            print(f"WARNING: Could not convert timestamp {event_timestamp_bigint}. Error: {e}")
            continue

        data_to_insert.append((
            event.get('user_pseudo_id'),
            event.get('event_name'),
            event_dt_str,
            event_timestamp_bigint,
            event.get('utm_source'),
            event.get('utm_campaign'),
            event.get('utm_medium'),
            json.dumps(event.get('event_params'))
        ))
    
    # Execute the batch insert if there is data to insert
    if data_to_insert:
        run_many_query(query_sql, data_to_insert)
