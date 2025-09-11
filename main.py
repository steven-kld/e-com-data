from fastapi import FastAPI
import threading
import time
from get_ga_db import query_last_ga_events, insert_ga_events
from get_shopify_sessions import extract_last_shopify_orders, insert_or_update_customer_from_order, insert_order_data
from match_orders import process_orders

# from dotenv import load_dotenv
# load_dotenv()

app = FastAPI()


def main_run():
    ga_events = query_last_ga_events()
    if ga_events:
        insert_ga_events(ga_events)

    orders_data = extract_last_shopify_orders()
    for order in orders_data:
        insert_or_update_customer_from_order(order)
        insert_order_data(order)
    process_orders()
    print("Task ended")

@app.get("/")
def index():
    return {"status": "running", "message": "Background job is active."}

@app.get("/run-db-update")
def run_db_update():
    main_run()
    return {"status": "ok", "message": "Job executed"}


