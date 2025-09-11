# e-com-data

Use GAS to trigger Vercel on 5 min basis

CREATE TABLE customers (
    shopify_customer_id BIGINT PRIMARY KEY,
    ga_user_pseudo_id TEXT[],
    shopify_customer_email TEXT,
    shopify_customer_phone TEXT,
    shopify_customer_first_name TEXT,
    shopify_customer_last_name TEXT,
    shopify_customer_created_at TIMESTAMP
);

CREATE TABLE orders (
    shopify_order_id BIGINT PRIMARY KEY,
    shopify_customer_id BIGINT,
    shopify_order_date TIMESTAMP,
    shopify_order_total NUMERIC(10, 2),
    shopify_delivery_price NUMERIC(10, 2),
    shopify_order_products JSONB,
    utm_source TEXT,
    utm_campaign TEXT,
    utm_medium TEXT,
    utm_term TEXT,
    ga_user_pseudo_id TEXT,
    CONSTRAINT fk_customer
        FOREIGN KEY(shopify_customer_id)
        REFERENCES customers(shopify_customer_id)
);

CREATE TABLE ga_events (
    ga_user_pseudo_id TEXT,
    event_name TEXT,
    event_timestamp TIMESTAMP,
    event_timestamp_numeric BIGINT,
    utm_source TEXT,
    utm_campaign TEXT,
    utm_medium TEXT,
    utm_term TEXT,
    event_params JSONB,
    PRIMARY KEY (ga_user_pseudo_id, event_timestamp)
);