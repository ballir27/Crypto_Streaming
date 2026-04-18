WITH flattened_events AS (

    SELECT
        id,
        jsonb_array_elements(data->'events') AS event_item,
        created_at
    FROM {{ source('project_4', 'coinbase_raw') }}
),

flattened_updates AS (
    SELECT
        id,
        jsonb_array_elements(event_item->'updates') AS update_item,
        created_at
    FROM flattened_events
)

SELECT
    id,
    (update_item->>'price_level')::numeric as price_level,
    (update_item->>'side')                 as side,
    (update_item->>'new_quantity')::numeric as new_quantity,
    (update_item->>'event_time')::timestamp as event_time,
    created_at::timestamp as time_ingested
from flattened_updates



        -- CAST(NULLIF(TRIM("Total_Population"), '') AS BIGINT) AS Total_Population,

        -- -- Age Groups (Converted to BigInt)
        -- CAST(NULLIF(TRIM("Under_5_Population"), '') AS BIGINT) AS Pop_Under_5,

        -- -- Geography (Kept as Strings/Ints depending on use)
        -- TRY_CAST(NULLIF(TRIM("state"), '') AS INT) AS State_Code,
        -- TRY_CAST(NULLIF(TRIM("county"), '') AS INT) AS County_Code,
        -- TRY_CAST(NULLIF(TRIM("fips_code"), '') AS INT) AS FIPS_Code