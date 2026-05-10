-- Revenue Growth & User Behavior Analysis
-- Final Project - GoIT Data Analytics
-- Author: Esra Can

WITH monthly_revenue AS (
    SELECT 
        p.user_id,
        p.game_name,
        u.language,
        u.has_older_device_model,
        u.age,
        DATE_TRUNC('month', p.payment_date)::date AS payment_month,
        SUM(p.revenue_amount_usd) AS total_revenue
    FROM project.games_payments p
    LEFT JOIN project.games_paid_users u ON p.user_id = u.user_id
    GROUP BY 1, 2, 3, 4, 5, 6
),

lag_data AS (
    SELECT 
        *,
        LAG(total_revenue) OVER (PARTITION BY user_id ORDER BY payment_month) AS prev_month_revenue,
        LAG(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS prev_payment_month,
        LEAD(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS next_payment_month
    FROM monthly_revenue
),

final_metrics AS (
    SELECT 
        *,
        CASE WHEN prev_payment_month IS NULL THEN total_revenue ELSE 0 END AS new_mrr,
        
        CASE WHEN prev_payment_month IS NOT NULL 
                  AND payment_month > (prev_payment_month + INTERVAL '1 month') 
             THEN total_revenue ELSE 0 END AS back_from_churn_revenue,
        
        CASE WHEN payment_month = (prev_payment_month + INTERVAL '1 month') 
                  AND total_revenue > prev_month_revenue 
             THEN total_revenue - prev_month_revenue ELSE 0 END AS expansion_revenue,
             
        CASE WHEN payment_month = (prev_payment_month + INTERVAL '1 month') 
                  AND total_revenue < prev_month_revenue 
             THEN prev_month_revenue - total_revenue ELSE 0 END AS contraction_revenue,

        CASE 
            WHEN next_payment_month IS NULL 
                 OR next_payment_month > (payment_month + INTERVAL '1 month')
            THEN total_revenue 
            ELSE 0 
        END AS churned_revenue,

        CASE 
            WHEN next_payment_month IS NULL 
                 OR next_payment_month > (payment_month + INTERVAL '1 month')
            THEN (payment_month + INTERVAL '1 month')::date
            ELSE NULL
        END AS churn_month
    FROM lag_data
)

SELECT * FROM final_metrics;
