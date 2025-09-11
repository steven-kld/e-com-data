import json 
from datetime import timedelta, datetime 
from db import run_query 

def query_orders_with_no_pseudo_ids():
    yesterday = datetime.now().date() - timedelta(days=1)
    orders = run_query( 
        """ 
        SELECT * FROM orders  
        WHERE ga_user_pseudo_id IS NULL and shopify_order_date > %s
        ORDER BY shopify_order_date
        """, (yesterday,), fetch_all=True 
    )
    return orders

def query_orders_on_date_range(order_with_no_ga_id): 
    if not order_with_no_ga_id: 
        print("No order provided to query against.") 
        return [] 

    # Get the data from the single order dictionary 
    base_date = order_with_no_ga_id['shopify_order_date'].date() 
    order_total = order_with_no_ga_id['shopify_order_total'] 
    delivery_price = order_with_no_ga_id['shopify_delivery_price'] 

    # The 'shopify_order_products' is already a Python list 
    shopify_products = order_with_no_ga_id['shopify_order_products'] 

    # Prepare a JSONB array for the SQL query, with the correct data types 
    ga_products_to_match = [ 
        { 
            "item_id": p['item_id'],  
            "price": float(p['price']), 
            "quantity": p['quantity'] 
        } 
        for p in shopify_products 
    ] 

    # Define the date range 
    start_date = base_date - timedelta(days=1) 
    end_date = base_date + timedelta(days=1) 

    query = """ 
        SELECT * FROM ga_events  
        WHERE 
            event_name IN ('purchase', 'form_submit', 'add_payment_info', 'add_shipping_info', 'begin_checkout', 'add_to_cart')
            AND DATE(event_timestamp) BETWEEN %s AND %s 
            AND (event_params->>'order_total')::numeric = %s 
            AND (event_params->>'shipping_value')::numeric = %s 
            AND event_params->'products' @> %s::jsonb 
    """ 
     
    purchases = run_query( 
        query,  
        (start_date, end_date, order_total, delivery_price, json.dumps(ga_products_to_match)),  
        fetch_all=True 
    ) 
     
    return purchases 

def get_last_events_by_pseudo_id(pseudo_id, is_referral=False):
    # AND utm_campaign != '(referral)' | optional condition, to be double-checked
    ref_line = "AND utm_campaign != '(referral)'"
    if is_referral:
        ref_line = ""
    query = f""" 
        SELECT event_name, event_timestamp, utm_source, utm_campaign, utm_medium, utm_term
        FROM ga_events
        WHERE ga_user_pseudo_id = '{pseudo_id}' AND utm_source IS NOT NULL 
        {ref_line}
        ORDER BY event_timestamp DESC
    """ 

    events = run_query( 
        query,
        fetch_all=True 
    ) 

    return events 

def update_order_with_pseudo_id_and_utms(shopify_order_id, pseudo_id, utm_source, utm_campaign, utm_medium, utm_term):
    query = """
    UPDATE orders
    SET 
        ga_user_pseudo_id = %s,
        utm_source = %s,
        utm_campaign = %s,
        utm_medium = %s,
        utm_term = %s
    WHERE shopify_order_id = %s
    """
    run_query(
        query,
        (
            pseudo_id,
            utm_source,
            utm_campaign,
            utm_medium,
            utm_term,
            shopify_order_id
        )
    )

def set_min_event_delta_utms(last_events, purchase_date_obj):
    min_event_delta_utms = { "delta": timedelta.max, "utms": {} } 

    if len(last_events) == 0: 
        return min_event_delta_utms

    for event in last_events:
        event_date = event.get('event_timestamp') 
        if isinstance(event_date, str): 
            event_date_obj = datetime.strptime(event_date, "%Y-%m-%d %H:%M:%S") 
        elif isinstance(event_date, datetime): 
            event_date_obj = event_date 
        else: 
            print(f"Skipping order due to unsupported date format: {event_date}") 
            continue 
        event_delta = purchase_date_obj - event_date_obj
        delta_in_seconds = event_delta.total_seconds()

        if delta_in_seconds < min_event_delta_utms.get('delta').total_seconds() and delta_in_seconds > 0: 
            min_event_delta_utms['delta'] = event_delta 
            min_event_delta_utms['utms'] = {
                "utm_source": event.get('utm_source', ""), 
                "utm_campaign": event.get('utm_campaign', ""), 
                "utm_medium": event.get('utm_medium', ""), 
                "utm_term": event.get('utm_term', "")
            }

        return min_event_delta_utms

def process_orders(): 
    orders = query_orders_with_no_pseudo_ids()

    if not orders: 
        print("No orders found without a GA pseudo ID to process.") 
        return 

    for order in orders: 
        print(f"Processing order: {order.get('shopify_order_id')}") 
        matched_purchases = query_orders_on_date_range(order) 

        if not matched_purchases: 
            print(order)
            print("No matching purchases found for this order. Continuing...") 
            print("---") 
            continue 

        min_delta = {"delta": timedelta.max, "ga_user_pseudo_id": None} 
         
        # Correctly handle a date that may be a string or a datetime object 
        order_date = order.get('shopify_order_date') 
        if isinstance(order_date, str): 
            order_date_obj = datetime.strptime(order_date, "%Y-%m-%d %H:%M:%S") 
        elif isinstance(order_date, datetime): 
            order_date_obj = order_date 
        else: 
            print(f"Skipping order due to unsupported date format: {order_date}") 
            print("---") 
            continue 

        for purchase in matched_purchases: 
            try: 
                purchase_date = purchase.get('event_timestamp') 
                if isinstance(purchase_date, str): 
                    purchase_date_obj = datetime.strptime(purchase_date, "%Y-%m-%d %H:%M:%S") 
                elif isinstance(purchase_date, datetime): 
                    purchase_date_obj = purchase_date 
                else: 
                    print(f"Skipping order due to unsupported date format: {purchase_date}") 
                    print("---") 
                    continue 

                # Calculate the absolute time difference 
                delta = abs(order_date_obj - purchase_date_obj) 
                delta_in_seconds = delta.total_seconds() 

                if delta_in_seconds < min_delta.get('delta').total_seconds(): 
                    min_delta['delta'] = delta 
                    min_delta['ga_user_pseudo_id'] = purchase.get('ga_user_pseudo_id') 
             
            except Exception as e: 
                print(f"Error processing purchase timestamp: {e}") 
                print("---") 
                continue 
        
        if min_delta.get('ga_user_pseudo_id'): 
            print(f"Matched GA purchase with delta {min_delta.get('delta')}: {min_delta.get('ga_user_pseudo_id')}")
            last_events = get_last_events_by_pseudo_id(min_delta.get('ga_user_pseudo_id'))
            min_event_delta_utms = set_min_event_delta_utms(last_events, purchase_date_obj)

            if min_event_delta_utms['utms'] == {}: 
                last_events = get_last_events_by_pseudo_id(min_delta.get('ga_user_pseudo_id'), True) # include (referral)
                min_event_delta_utms = set_min_event_delta_utms(last_events, purchase_date_obj)

            if min_event_delta_utms.get('utms') != {}:
                print(min_event_delta_utms.get('utms'))
            else:
                print("no utms found")

            update_order_with_pseudo_id_and_utms(
                shopify_order_id=order.get('shopify_order_id'), 
                pseudo_id=min_delta.get('ga_user_pseudo_id'), 
                utm_campaign=min_event_delta_utms.get('utms').get('utm_campaign', ""),
                utm_source=min_event_delta_utms.get('utms').get('utm_source', ""),
                utm_medium=min_event_delta_utms.get('utms').get('utm_medium', ""),
                utm_term=min_event_delta_utms.get('utms').get('utm_term', "")
                )
        else: 
            print("Could not find a valid match for this order.")
         
        print("---") 


