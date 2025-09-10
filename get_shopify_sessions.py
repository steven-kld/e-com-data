import os
import json
import requests
from dotenv import load_dotenv
from db import run_query

load_dotenv()

def get_orders_data(api_key: str, domain: str, created_at_min: str):
    orders_url = f"https://{domain}/admin/api/2023-10/orders.json"
    headers = {
        "X-Shopify-Access-Token": api_key,
        "Content-Type": "application/json"
    }
    
    orders_params = {
        "created_at_min": created_at_min
    }

    try:
        orders_response = requests.get(orders_url, headers=headers, params=orders_params)
        orders_response.raise_for_status()
        orders = orders_response.json().get('orders', [])
        
        orders_data = []
        for order in orders:
            # Safely access the customer object
            customer = order.get('customer', {})
            customer_email = customer.get('email')
            
            # --- Access customer name and creation date ---
            customer_first_name = customer.get('first_name')
            customer_last_name = customer.get('last_name')
            customer_created_at = customer.get('created_at')

            # --- Access phone from billing_address ---
            billing_address = order.get('billing_address', {})
            customer_phone = billing_address.get('phone')

            # --- Get product names and URLs ---
            products_list = []
            line_items = order.get('line_items', [])
            for item in line_items:
                product_id = item.get('product_id')
                product_name = item.get('name')
                product_price = item.get('price')
                product_quantity = item.get('quantity')
                product_url = f"https://{domain}/products/{product_id}" if product_id else "N/A"

                products_list.append({
                    "name": product_name,
                    "url": product_url,
                    "price": product_price,
                    "quantity": product_quantity
                })
            products_total_price = 0
            for item in line_items: 
                products_total_price += float(item.get('price', '0.0')) * float(item.get('quantity', '0.0'))
            delivery_price = float(order.get('total_price', '0.0')) - float(products_total_price)

            order_data = {
                "orderId": order.get('id'),
                "customerId": customer.get('id'),
                "customerEmail": customer_email,
                "customerPhone": customer_phone,
                "customerFirstName": customer_first_name,
                "customerLastName": customer_last_name,
                "customerCreatedAt": customer_created_at,
                "orderDate": order.get('created_at'),
                "orderTotal": order.get('total_price'),
                "orderDeliveryPrice": delivery_price,
                "products": products_list
            }
            orders_data.append(order_data)
        
        return orders_data
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while calling the Shopify API: {e}")
        return None

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
