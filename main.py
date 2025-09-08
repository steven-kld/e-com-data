from get_ga_db import query_last_ga_events, insert_ga_events
from get_shopify_sessions import extract_last_shopify_orders, insert_or_update_customer_from_order, insert_order_data

def main_run():
    ga_events = query_last_ga_events()
    if ga_events:
        insert_ga_events(ga_events)

    orders_data = extract_last_shopify_orders()
    print(orders_data)
    for order in orders_data:
        insert_or_update_customer_from_order(order)
        insert_order_data(order)

main_run()