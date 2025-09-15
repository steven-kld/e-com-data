import os, time, requests, json, re
from db import run_query
import pandas as pd

def get_orders_data(api_key: str, domain: str, created_at_min: str):
    orders_url = f"https://{domain}/admin/api/2023-10/orders.json"
    headers = {
        "X-Shopify-Access-Token": api_key,
        "Content-Type": "application/json"
    }

    all_orders = []
    initial_params = {
        "limit": 50,
        "created_at_min": created_at_min
    }
    

    next_page_url = f"{orders_url}?{requests.compat.urlencode(initial_params)}"

    while next_page_url:
        try:
            orders_response = requests.get(next_page_url, headers=headers)
            orders_response.raise_for_status()
            orders_page = orders_response.json().get('orders', [])

            if not orders_page:
                break

            for order in orders_page:
                customer = order.get('customer', {})
                customer_email = customer.get('email')
                customer_first_name = customer.get('first_name')
                customer_last_name = customer.get('last_name')
                customer_created_at = customer.get('created_at')
                billing_address = order.get('billing_address', {})
                customer_phone = billing_address.get('phone')
                products_list = []
                products_total_price = 0.0
                line_items = order.get('line_items', [])
                for item in line_items:
                    product_id = item.get('product_id')
                    product_price = float(item.get('price', '0.0'))
                    product_quantity = float(item.get('quantity', '0.0'))
                    
                    products_list.append({
                        "item_id": product_id,
                        "price": product_price,
                        "quantity": product_quantity
                    })
                    products_total_price += product_price * product_quantity

                order_total = float(order.get('total_price', '0.0'))
                delivery_price = order_total - products_total_price
                
                order_data = {
                    "orderId": order.get('id'),
                    "landingSite": order.get('landing_site'),
                    "customerId": customer.get('id'),
                    "customerEmail": customer_email,
                    "customerPhone": customer_phone,
                    "customerFirstName": customer_first_name,
                    "customerLastName": customer_last_name,
                    "customerCreatedAt": customer_created_at,
                    "orderDate": order.get('created_at'),
                    "orderTotal": order_total,
                    "orderDeliveryPrice": delivery_price,
                    "products": products_list
                }
                all_orders.append(order_data)

            link_header = orders_response.headers.get('Link')
            
            # Use regex to find the 'rel="next"' URL reliably
            next_url_match = re.search(r'<(.*?)>; rel="next"', link_header if link_header else '')
            
            if next_url_match:
                next_page_url = next_url_match.group(1)
            else:
                next_page_url = None
            
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"Failed to get orders: {e}")
            return None

    return all_orders


def extract_last_shopify_orders():
    last_order_date = run_query(
        """
        SELECT shopify_order_date
        FROM orders 
        ORDER BY shopify_order_date DESC
        LIMIT 1;
        """, (), fetch_one=True
    ).get('shopify_order_date')

    shopify_api_key = os.getenv('SHOPIFY_API_KEY')
    shopify_domain = os.getenv('SHOPIFY_DOMAIN')
    
    if not shopify_api_key or not shopify_domain:
        print("Shopify credentials not found. Please set SHOPIFY_API_KEY and SHOPIFY_DOMAIN in your .env file.")
    else:
        # Get the current date and set the time to 00:00:00
        start_date_iso = last_order_date.isoformat()
        
        print(f"Fetching new purchases from Shopify since {start_date_iso}...")
        orders_data = get_orders_data(shopify_api_key, shopify_domain, start_date_iso)
        
        if orders_data:
            return orders_data
        else:
            print("No new purchases found or an error occurred.")

def insert_or_update_customer_from_order(order_data):
    run_query(
        """
        INSERT INTO customers (
            shopify_customer_id,
            shopify_customer_email,
            shopify_customer_phone,
            shopify_customer_first_name,
            shopify_customer_last_name,
            shopify_customer_created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (shopify_customer_id) 
        DO UPDATE SET
            shopify_customer_email = EXCLUDED.shopify_customer_email,
            shopify_customer_phone = EXCLUDED.shopify_customer_phone,
            shopify_customer_first_name = EXCLUDED.shopify_customer_first_name,
            shopify_customer_last_name = EXCLUDED.shopify_customer_last_name
        """,
        (
            order_data.get('customerId'),
            order_data.get('customerEmail'),
            order_data.get('customerPhone'),
            order_data.get('customerFirstName'),
            order_data.get('customerLastName'),
            order_data.get('customerCreatedAt')
        )
    )

def insert_order_data(order_data):
    run_query(
        """
        INSERT INTO orders (
            shopify_order_id,
            shopify_customer_id,
            shopify_order_date,
            shopify_order_total,
            shopify_delivery_price,
            shopify_order_products
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (shopify_order_id) DO NOTHING
        """,
        (
            order_data.get('orderId'),
            order_data.get('customerId'),
            order_data.get('orderDate'),
            order_data.get('orderTotal'),
            order_data.get('orderDeliveryPrice'),
            json.dumps(order_data.get('products'))
        )
    )

def get_products_by_ids(item_ids_dict):
    def clean_handle(handle):
        if pd.isna(handle) or not isinstance(handle, str):
            return handle
            
        # Remove Python-style Unicode escape sequences
        cleaned_handle = re.sub(r'\\u[0-9a-fA-F]{4}', '', handle)
        
        # Remove raw Unicode emojis
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"
            "\U0001F300-\U0001F5FF"
            "\U0001F680-\U0001F6FF"
            "\U0001F700-\U0001F77F"
            "\U0001F780-\U0001F7FF"
            "\U0001F800-\U0001F8FF"
            "\U0001F900-\U0001F9FF"
            "\U0001FA00-\U0001FA6F"
            "\U0001FA70-\U0001FAFF"
            "\U00002702-\U000027B0"
            "]+", flags=re.UNICODE)
            
        final_handle = emoji_pattern.sub(r'', cleaned_handle)
        
        # Remove any other non-standard characters, leaving only a-z, 0-9, and dashes.
        final_handle = re.sub(r'[^a-zA-Z0-9\-]', '', final_handle)
        
        return final_handle

    if not item_ids_dict:
        return {}
    
    shopify_api_key = os.getenv('SHOPIFY_API_KEY')
    shopify_domain = os.getenv('SHOPIFY_DOMAIN')
    
    products_url = f"https://{shopify_domain}/admin/api/2023-10/products.json"
    headers = {
        "X-Shopify-Access-Token": shopify_api_key,
        "Content-Type": "application/json"
    }
    
    all_products_with_handles = {}
    chunk_size = 50
    
    item_ids = [item_id for item_id in item_ids_dict.keys() if item_id is not None]
    
    for i in range(0, len(item_ids), chunk_size):
        chunk = item_ids[i:i + chunk_size]
        ids_string = ",".join(map(str, chunk))
        
        params = {
            "ids": ids_string
        }

        try:
            response = requests.get(products_url, headers=headers, params=params)
            response.raise_for_status()

            products_page = response.json().get('products', [])
            for product in products_page:
                product_id = product.get('id')
                product_handle = product.get('handle')

                if product_id and product_handle:
                    # Apply cleaning function before storing
                    cleaned_handle = clean_handle(product_handle)
                    all_products_with_handles[product_id] = cleaned_handle

            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching products for chunk starting at index {i}: {e}")
            return {}
            
    return all_products_with_handles

