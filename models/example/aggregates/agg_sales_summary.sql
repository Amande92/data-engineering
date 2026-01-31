WITH sales AS (

    SELECT
        f.order_id,
        s.category,
        s.country,
        f.quantity,
        f.total_amount
    FROM {{ ref('fct_sales') }} f
    JOIN {{ ref('stg_sales') }} s
      ON f.order_id = s.order_id
),

final AS (

    SELECT
        
        category         AS category,
        country         AS country,

        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity)            AS total_quantity,
        SUM(total_amount)        AS total_sales_amount,
        AVG(total_amount)        AS average_order_value

    FROM sales
    GROUP BY category, country
)

SELECT *
FROM final


