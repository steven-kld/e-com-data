from flask import Flask, jsonify
import threading
import time

from get_ga_db import query_last_ga_events, insert_ga_events
from get_shopify_sessions import extract_last_shopify_orders, insert_or_update_customer_from_order, insert_order_data
from match_orders import process_orders

app = Flask(__name__)

def main_run():
    ga_events = query_last_ga_events()
    if ga_events:
        insert_ga_events(ga_events)

    orders_data = extract_last_shopify_orders()
    for order in orders_data:
        insert_or_update_customer_from_order(order)
        insert_order_data(order)
    process_orders()

def background_worker():
    """Run main_run every 3 minutes."""
    while True:
        try:
            print("Running main_run()...")
            main_run()
            print("main_run() completed. Sleeping 3 minutes...")
        except Exception as e:
            print(f"Error in main_run: {e}")
        time.sleep(180)  # 3 minutes

@app.route("/")
def index():
    return jsonify({"status": "running", "message": "Background job is active."})

if __name__ == "__main__":
    # Start background thread
    worker_thread = threading.Thread(target=background_worker, daemon=True)
    worker_thread.start()
    # Start Flask server
    app.run(host="0.0.0.0", port=5000)
