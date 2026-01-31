WITH source_data AS (

    SELECT
        order_id,
        customer_name,
        product,
        category,
        country,
        quantity,
        unit_price,
        order_date,
        created_at
    FROM {{ source('raw', 'sales_raw') }}

),

cleaned AS (

    SELECT
        order_id                             AS order_id,

        TRIM(customer_name)                 AS customer_name,
        TRIM(product)                       AS product,

        quantity::integer                   AS quantity,
        unit_price::numeric                 AS unit_price,

        order_date::date                    AS order_date,
        created_at::timestamp               AS created_at

    FROM source_data

    WHERE order_id IS NOT NULL
      AND quantity > 0
      AND unit_price > 0
)

SELECT *
FROM cleaned
