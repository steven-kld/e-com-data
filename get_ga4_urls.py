import os, re, json
from urllib.parse import urlparse, parse_qs, urlunparse
from datetime import date
import pandas as pd
from get_shopify_sessions import get_orders_data, get_products_by_ids

from dotenv import load_dotenv
load_dotenv()

GA_EVENTS_TABLE = os.getenv('GA_EVENTS_TABLE')
ORG_TIMEZONE = os.getenv('ORG_TIMEZONE')

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

def ads_raw_report_to_df(raw_path='ads_url_report.csv'):
    ads_df = load_ads_data(raw_path)
    required_columns = ['segments.date', 'campaign.name', 'expanded_landing_page_view.expanded_final_url', 'metrics.impressions', 'metrics.clicks', 'metrics.conversions', 'metrics.cost_micros']
    
    if ads_df is None or not all(col in ads_df.columns for col in required_columns):
        print('missing cols')
        return pd.DataFrame()
    else:
        ads_df['campaign'] = ads_df['campaign.name']
        ads_df['gbraid'] = ads_df['expanded_landing_page_view.expanded_final_url'].apply(extract_gbraid)
        ads_df['gad_campaignid'] = ads_df['expanded_landing_page_view.expanded_final_url'].apply(extract_gad_campaignid)
        ads_df['clean_url'] = ads_df['expanded_landing_page_view.expanded_final_url'].apply(clean_url)
        ads_df['target_page'] = ads_df['clean_url'].apply(get_target_page)
        
        # Convert numeric columns to float first to handle NaN values
        ads_df['metrics.impressions'] = pd.to_numeric(ads_df['metrics.impressions'], errors='coerce')
        ads_df['metrics.clicks'] = pd.to_numeric(ads_df['metrics.clicks'], errors='coerce')
        ads_df['metrics.conversions'] = pd.to_numeric(ads_df['metrics.conversions'], errors='coerce')
        ads_df['metrics.cost_micros'] = pd.to_numeric(ads_df['metrics.cost_micros'], errors='coerce')
        ads_df['metrics.cost'] = (ads_df['metrics.cost_micros'] / 1_000_000)
        
        if ads_df is not None and not ads_df.empty and 'campaign.name' in ads_df.columns:
            ads_df['utm_campaign'] = ads_df['campaign.name'].str.lower().str.replace(' ', '_').str.replace(r'(\d{2})\.(\d{2})\.\d{4}', r'\1\2', regex=True).str.replace('.', '')
        
        aggregated_df = ads_df.groupby(['target_page', 'gad_campaignid', 'campaign']).agg({
            'metrics.impressions': 'sum',
            'metrics.clicks': 'sum',
            'metrics.conversions': 'sum',
            'metrics.cost': 'sum'
        }).reset_index()

        # Fill any NaN values with 0 before converting to int
        aggregated_df[['metrics.impressions', 'metrics.clicks', 'metrics.conversions', 'metrics.cost']] = aggregated_df[['metrics.impressions', 'metrics.clicks', 'metrics.conversions', 'metrics.cost']].fillna(0)

        # Convert the specified numeric columns to integers
        aggregated_df['metrics.impressions'] = aggregated_df['metrics.impressions'].astype(int)
        aggregated_df['metrics.clicks'] = aggregated_df['metrics.clicks'].astype(int)
        aggregated_df['metrics.conversions'] = aggregated_df['metrics.conversions'].astype(int)
        
        # NOTE: `metrics.cost` will remain a float as it represents currency and may have decimal places. 
        # Converting it to an int would truncate the value. If you want to keep it as an int,
        # you can round it or multiply it by 100 to keep cents.

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

def add_comment_column(df):
    if df.empty or 'cost' not in df.columns or 'conv' not in df.columns or 'click' not in df.columns:
        raise ValueError("DataFrame is empty or missing required columns.")

    # Filter for campaigns that have conversions to calculate meaningful averages
    converting_campaigns = df[df['conv'] > 0].copy()

    # Calculate average CPA and CPC for converting campaigns
    total_cost_conv = converting_campaigns['cost'].sum()
    total_conversions_conv = converting_campaigns['conv'].sum()
    total_clicks_conv = converting_campaigns['click'].sum()

    acpa = total_cost_conv / total_conversions_conv if total_conversions_conv > 0 else 0
    acpc = total_cost_conv / total_clicks_conv if total_clicks_conv > 0 else 0

    # Calculate CPA and CPC for all campaigns, handling zero values
    df['cpa'] = df.apply(
        lambda row: row['cost'] / row['conv'] if row['conv'] > 0 else row['cost'],
        axis=1
    )
    df['cpc'] = df.apply(
        lambda row: row['cost'] / row['click'] if row['click'] > 0 else row['cost'],
        axis=1
    )

    # Initialize is_waste column with an empty string
    df['comment'] = ''
    
    # Base condition: Campaigns that spend little money but don't have conversions
    df.loc[(df['conv'] == 0) & (df['cost'] > 0), 'comment'] = 'Little spend, CPC ~avg'

    # Condition 1: CPC-based waste for non-converting campaigns
    df.loc[(df['conv'] == 0) & (df['cpc'] > (acpc * 1.5)), 'comment'] = 'CPC > 1.5 of avg with no conv'
    
    # Condition 2: CPA-based waste for non-converting campaigns
    df.loc[(df['conv'] == 0) & (df['cost'] > acpa * 0.5), 'comment'] = 'Zero conversions, too high spend'
    
    # Condition 3: CPA-based waste for converting campaigns
    df.loc[(df['conv'] > 0) & (df['cpa'] > (acpa * 1.5)), 'comment'] = 'CPA > 1.5 of avg'

    # Condition 4: CPA-based rate for converting campaigns
    df.loc[(df['conv'] > 0) & (df['cpa'] < acpa), 'comment'] = 'CPA < avg, brilliant'

    # Condition 5: CPA-based rate for converting campaigns
    df.loc[(df['conv'] > 0) & (df['cpa'] >= acpa) & (df['cpa'] <= (acpa * 1.5)), 'comment'] = 'CPA ~avg'

    return df

def get_product_ad_spend(final_report_df, product_revenue):
    required_cols = ['target_page', 'imp', 'click', 'conv', 'cost']
    if not all(col in final_report_df.columns for col in required_cols):
        raise ValueError(f"DataFrame is missing one or more required columns: {required_cols}")

    for handle in product_revenue.keys():
        product_path = f"/products/{handle}"
        
        # Filter the DataFrame for rows where target_page matches the product path
        direct_spend_df = final_report_df[final_report_df['target_page'] == product_path]
        
        # Sum the metrics for the filtered DataFrame
        if not direct_spend_df.empty:
            # Cast the summed values to standard Python types
            total_imp = int(direct_spend_df['imp'].sum())
            total_click = int(direct_spend_df['click'].sum())
            total_conv = int(direct_spend_df['conv'].sum())
            total_cost = float(direct_spend_df['cost'].sum())
            
            # Add the new metrics to the existing product_revenue entry
            product_revenue[handle]['imp'] = total_imp
            product_revenue[handle]['click'] = total_click
            product_revenue[handle]['conv'] = total_conv
            product_revenue[handle]['cost'] = total_cost
        else:
            # If no direct campaigns found, add zero values
            product_revenue[handle]['imp'] = 0
            product_revenue[handle]['click'] = 0
            product_revenue[handle]['conv'] = 0
            product_revenue[handle]['cost'] = 0
            
    return product_revenue

def summarize_by_urls(products_urls_df):
    urls_dict = {}
    for row in products_urls_df.itertuples():
        urls_dict[row.target_page] = {
            "cost": row.cost,
            "revenue": row.rev,
            "purchases": row.conv
        }

    print({
        "cost": products_urls_df['cost'].sum(),
        "revenue": products_urls_df['rev'].sum(),
        "purchases": products_urls_df['conv'].sum()
    })

    products = {
        "cost": products_urls_df['cost'].sum(),
        "revenue": products_urls_df['rev'].sum(),
        "purchases": products_urls_df['conv'].sum(),
        "urls": urls_dict
    }

    return products

def summarize_all(final_report_df, orders):
    product_revenue = {}
    for order in orders:
        for product in order.get('products', []):
            handle = product.get('handle')
            net_revenue = product.get('price', 0.0)
            if handle:
                product_data = product_revenue.get(handle, {"revenue": 0.0, "price": net_revenue})

                product_data["revenue"] += net_revenue
                product_revenue[handle] = product_data

    product_revenue_spend_metrics = get_product_ad_spend(final_report_df, product_revenue)
    core_products_to_scale_urls = {
        handle: data for handle, data in product_revenue_spend_metrics.items()
        if data.get('revenue', 0) > data.get('price', 0) * 2 and data.get('conv', 0) * data.get('price', 0) * 2 <= data.get('revenue', 0)
    }
    
    core_products_to_scale = {
        "total_revenue": sum(data['revenue'] for data in core_products_to_scale_urls.values()),
        "direct_cost": sum(data['cost'] for data in core_products_to_scale_urls.values()),
        "direct_revenue": sum(data['conv'] * data['price'] for data in core_products_to_scale_urls.values()),
        "urls": core_products_to_scale_urls
    }

    brilliant_urls = summarize_by_urls(
        ads_df[
            (ads_df['comment'].isin(['CPA < avg, brilliant', 'CPA ~avg'])) &
            (ads_df['conv'] > 1) & (ads_df['target_page'].str.contains('product', case=False, na=False))
        ]
    )

    wasting_urls = summarize_by_urls(
        ads_df[
            (ads_df['comment'].isin(['Zero conversions, too high spend', 'CPC > 1.5 of avg with no conv', 'CPA > 1.5 of avg'])) &
            (ads_df['target_page'] != "") & 
            (ads_df['target_page'] != "/")
        ]
    )
    
    final_report_totals = {
        "total_cost": final_report_df['cost'].sum(),
        "total_revenue": final_report_df['rev'].sum(),
        "total_conversions": final_report_df['conv'].sum()
    }

    return core_products_to_scale, brilliant_urls, wasting_urls, final_report_totals

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

        final_report_df.rename(columns={
            'gad_campaignid': 'gad',
            'metrics.impressions': 'imp',
            'metrics.clicks': 'click',
            'metrics.cost': 'cost',
            'total_revenue': 'rev',
            'total_purchases': 'conv'
        }, inplace=True)
        final_report_df.drop(columns=['metrics.conversions'], inplace=True)

        add_comment_column(final_report_df)        

        core_products_to_scale, brilliant_urls, wasting_urls, final_report_totals = summarize_all(final_report_df, orders)

        print("Products that have purchases and perform well, but have poor or none direct traffic")
        print(json.dumps(core_products_to_scale, indent=2))
        print("Products with brilliant performance")
        print(json.dumps(brilliant_urls, indent=2))
        print("Products with poor performance")
        print(json.dumps(wasting_urls, indent=2))
        print("Total values")
        print(json.dumps(final_report_totals, indent=2))

        # output_file_name = 'final_ads_ga4_report.xlsx'
        # final_report_df.to_excel(output_file_name, index=False)
        # print(f"Report saved: {output_file_name}")
    else:
        print("No Ads data or Google-sourced orders found.")
