DROP TABLE IF EXISTS minio.tiny.web_events;
DROP TABLE IF EXISTS minio.tiny.sub_events;

CREATE SCHEMA IF NOT EXISTS minio.tiny  WITH (location = 's3a://tiny/');

CREATE TABLE IF NOT EXISTS minio.tiny.web_events (
    event_name VARCHAR,
    event_time TIMESTAMP, 
    user_id VARCHAR, 
    user_properties ROW(
        aquisition_campaign VARCHAR, 
        country_code VARCHAR,
        is_subscribed BOOLEAN,
        locale VARCHAR,
        user_id VARCHAR
    ),
    event_properties ROW(
        search_term VARCHAR,
        url VARCHAR,
        item_id VARCHAR,
        price_shown INTEGER,
        cart_size INTEGER
    )
) WITH (
    format = 'PARQUET', 
    external_location = 's3a://tiny/web_events'
);

CREATE TABLE IF NOT EXISTS  minio.tiny.sub_events (
    subscription_time TIMESTAMP, 
    subscriber_id VARCHAR, 
    event_properties ROW(reason VARCHAR)
) WITH (
    format = 'PARQUET', 
    external_location = 's3a://tiny/sub_events'
);


SELECT * FROM minio.tiny.sub_events limit 1;
SELECT * FROM minio.tiny.web_events limit 1;
