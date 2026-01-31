WITH sales AS (

    SELECT
        order_id,
        quantity,
        unit_price,
        order_date
    FROM {{ ref('stg_sales') }}

),

final AS (

    SELECT
        order_id,

        SUM(quantity) AS quantity,
        AVG(unit_price) AS unit_price,

        SUM(quantity * unit_price) AS total_amount,

        MIN(order_date) AS order_date,

        EXTRACT(YEAR FROM MIN(order_date))  AS order_year,
        EXTRACT(MONTH FROM MIN(order_date)) AS order_month,

        CASE
            WHEN SUM(quantity * unit_price) > 500 THEN TRUE
            ELSE FALSE
        END AS is_high_value_order

    FROM sales
    GROUP BY order_id
)

SELECT *
FROM final
