SELECT
    date_trunc('minute', event_time) as minute_bucket,
    SUM(new_quantity) as total_volume,
    AVG(price_level) as average_price,
    CASE 
        WHEN SUM(new_quantity) > 0 
        THEN SUM(price_level * new_quantity) / SUM(new_quantity) 
        ELSE 0 
    END as vwap,
    MAX(price_level) as high_price,
    MIN(price_level) as low_price
FROM {{ ref('stg_crypto') }}
GROUP BY minute_bucket