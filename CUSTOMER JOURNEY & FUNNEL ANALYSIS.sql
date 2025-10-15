-- =============================================
-- CUSTOMER JOURNEY & FUNNEL ANALYSIS
-- Skills: Window Functions, CTE, Recursive Queries, Funnel Analysis, 
--         Cohort Analysis, Path Analysis, RFM, Customer Lifetime Value
-- =============================================

-- =============================================
-- CUSTOMER FUNNEL ANALYSIS
-- =============================================

WITH funnel_steps AS (
    SELECT 
        customer_id,
        session_id,
        MAX(CASE WHEN page_url = '/home' THEN 1 ELSE 0 END) AS reached_home,
        MAX(CASE WHEN page_url LIKE '/product/%' THEN 1 ELSE 0 END) AS viewed_product,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
        MAX(CASE WHEN page_url = '/checkout' THEN 1 ELSE 0 END) AS reached_checkout,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS made_purchase
    FROM page_events
    GROUP BY customer_id, session_id
),
funnel_aggregation AS (
    SELECT 
        COUNT(DISTINCT session_id) AS total_sessions,
        SUM(reached_home) AS step_home,
        SUM(viewed_product) AS step_products,
        SUM(added_to_cart) AS step_cart,
        SUM(reached_checkout) AS step_checkout,
        SUM(made_purchase) AS step_purchase
    FROM funnel_steps
),
funnel_conversion AS (
    SELECT 
        total_sessions,
        step_home,
        step_products,
        step_cart,
        step_checkout,
        step_purchase,
        ROUND((step_home * 100.0 / total_sessions), 2) AS home_conversion_rate,
        ROUND((step_products * 100.0 / step_home), 2) AS product_conversion_rate,
        ROUND((step_cart * 100.0 / step_products), 2) AS cart_conversion_rate,
        ROUND((step_checkout * 100.0 / step_cart), 2) AS checkout_conversion_rate,
        ROUND((step_purchase * 100.0 / step_checkout), 2) AS purchase_conversion_rate,
        ROUND((step_purchase * 100.0 / total_sessions), 2) AS overall_conversion_rate
    FROM funnel_aggregation
)
SELECT 
    'Home Page' AS funnel_step,
    step_home AS sessions,
    home_conversion_rate AS conversion_rate
FROM funnel_conversion
UNION ALL
SELECT 
    'Product View',
    step_products,
    product_conversion_rate
FROM funnel_conversion
UNION ALL
SELECT 
    'Add to Cart',
    step_cart,
    cart_conversion_rate
FROM funnel_conversion
UNION ALL
SELECT 
    'Checkout',
    step_checkout,
    checkout_conversion_rate
FROM funnel_conversion
UNION ALL
SELECT 
    'Purchase',
    step_purchase,
    purchase_conversion_rate
FROM funnel_conversion
UNION ALL
SELECT 
    'Overall',
    step_purchase,
    overall_conversion_rate
FROM funnel_conversion;

-- =============================================
-- CUSTOMER JOURNEY PATH ANALYSIS
-- Identify Most Common User Paths
-- =============================================

WITH session_journeys AS (
    SELECT 
        session_id,
        customer_id,
        GROUP_CONCAT(
            CASE 
                WHEN page_url = '/home' THEN 'Home'
                WHEN page_url LIKE '/product/%' THEN 'Product'
                WHEN page_url = '/cart' THEN 'Cart'
                WHEN page_url = '/checkout' THEN 'Checkout'
                WHEN page_url = '/order/confirmation' THEN 'Confirmation'
                ELSE 'Other'
            END
            ORDER BY event_timestamp
            SEPARATOR ' → '
        ) AS customer_path,
        COUNT(*) AS steps_in_path,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS converted
    FROM page_events
    GROUP BY session_id, customer_id
),
path_analysis AS (
    SELECT 
        customer_path,
        COUNT(*) AS path_frequency,
        ROUND(AVG(steps_in_path), 2) AS avg_steps,
        SUM(converted) AS conversions,
        ROUND(SUM(converted) * 100.0 / COUNT(*), 2) AS conversion_rate
    FROM session_journeys
    GROUP BY customer_path
    HAVING COUNT(*) >= 1
)
SELECT 
    customer_path AS user_journey,
    path_frequency AS frequency,
    avg_steps,
    conversions,
    conversion_rate AS conversion_pct
FROM path_analysis
ORDER BY path_frequency DESC, conversion_rate DESC;

-- =============================================
-- ADVANCED COHORT ANALYSIS
-- Monthly Cohort Retention with Revenue
-- =============================================

WITH customer_cohorts AS (
    SELECT 
        customer_id,
        DATE_FORMAT(signup_date, '%Y-%m') AS signup_month,
        DATE_FORMAT(MIN(o.order_timestamp), '%Y-%m') AS first_purchase_month
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY customer_id, DATE_FORMAT(signup_date, '%Y-%m')
),
monthly_activity AS (
    SELECT 
        cc.customer_id,
        cc.signup_month,
        DATE_FORMAT(o.order_timestamp, '%Y-%m') AS activity_month,
        COUNT(o.order_id) AS order_count,
        SUM(o.total_amount) AS total_revenue
    FROM customer_cohorts cc
    LEFT JOIN orders o ON cc.customer_id = o.customer_id 
        AND DATE_FORMAT(o.order_timestamp, '%Y-%m') >= cc.signup_month
    GROUP BY cc.customer_id, cc.signup_month, DATE_FORMAT(o.order_timestamp, '%Y-%m')
),
cohort_analysis AS (
    SELECT 
        signup_month,
        activity_month,
        COUNT(DISTINCT customer_id) AS active_customers,
        SUM(total_revenue) AS monthly_revenue,
        ROUND(SUM(total_revenue) / COUNT(DISTINCT customer_id), 2) AS arpu
    FROM monthly_activity
    WHERE activity_month IS NOT NULL
    GROUP BY signup_month, activity_month
),
cohort_pivot AS (
    SELECT 
        signup_month,
        MAX(CASE WHEN activity_month = signup_month THEN active_customers END) AS month_0,
        MAX(CASE WHEN activity_month = DATE_FORMAT(DATE_ADD(STR_TO_DATE(CONCAT(signup_month, '-01'), '%Y-%m-%d'), INTERVAL 1 MONTH), '%Y-%m') THEN active_customers END) AS month_1,
        MAX(CASE WHEN activity_month = DATE_FORMAT(DATE_ADD(STR_TO_DATE(CONCAT(signup_month, '-01'), '%Y-%m-%d'), INTERVAL 2 MONTH), '%Y-%m') THEN active_customers END) AS month_2
    FROM cohort_analysis
    GROUP BY signup_month
)
SELECT 
    signup_month AS cohort,
    month_0 AS m0_customers,
    month_1 AS m1_customers,
    month_2 AS m2_customers,
    ROUND((month_1 * 100.0 / month_0), 2) AS m1_retention_rate,
    ROUND((month_2 * 100.0 / month_0), 2) AS m2_retention_rate
FROM cohort_pivot
ORDER BY signup_month;

-- =============================================
-- CUSTOMER LIFETIME VALUE (CLV) PREDICTION
-- Advanced CLV with RFM Segmentation
-- =============================================

WITH customer_rfm AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.signup_date,
        c.acquisition_channel,
        -- Recency (days since last purchase)
        COALESCE(DATEDIFF('2024-04-30', MAX(o.order_timestamp)), 999) AS recency_days,
        -- Frequency (total orders)
        COUNT(o.order_id) AS frequency,
        -- Monetary (total spend)
        COALESCE(SUM(o.total_amount), 0) AS monetary,
        -- Average Order Value
        CASE 
            WHEN COUNT(o.order_id) > 0 THEN COALESCE(SUM(o.total_amount), 0) / COUNT(o.order_id)
            ELSE 0
        END AS avg_order_value
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name, c.signup_date, c.acquisition_channel
),
rfm_scores AS (
    SELECT *,
        -- RFM Scoring (1-5, 5 being best)
        NTILE(5) OVER (ORDER BY recency_days DESC) AS recency_score,
        NTILE(5) OVER (ORDER BY frequency) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary) AS monetary_score
    FROM customer_rfm
),
rfm_segments AS (
    SELECT *,
        CONCAT(recency_score, frequency_score, monetary_score) AS rfm_cell,
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 THEN 'New Customers'
            WHEN frequency_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost Customers'
            ELSE 'Need Attention'
        END AS rfm_segment
    FROM rfm_scores
),
clv_calculation AS (
    SELECT 
        rfm_segment,
        COUNT(*) AS customer_count,
        ROUND(AVG(monetary), 2) AS avg_revenue,
        ROUND(AVG(frequency), 2) AS avg_frequency,
        ROUND(AVG(avg_order_value), 2) AS avg_order_value,
        -- Simple CLV: Avg Revenue per Customer * Purchase Frequency * Customer Lifespan (assumed 1 year)
        ROUND(AVG(monetary) * AVG(frequency) * 1, 2) AS predicted_clv_1yr
    FROM rfm_segments
    GROUP BY rfm_segment
)
SELECT 
    rfm_segment,
    customer_count,
    avg_revenue,
    avg_frequency,
    avg_order_value,
    predicted_clv_1yr,
    ROUND((customer_count * 100.0 / (SELECT COUNT(*) FROM rfm_segments)), 2) AS segment_percentage
FROM clv_calculation
ORDER BY predicted_clv_1yr DESC;

-- =============================================
-- PRODUCT AFFINITY ANALYSIS
-- Market Basket Analysis & Product Recommendations
-- =============================================

WITH product_pairs AS (
    SELECT 
        oi1.product_id AS product_a,
        oi2.product_id AS product_b,
        p1.product_name AS product_a_name,
        p2.product_name AS product_b_name,
        COUNT(DISTINCT oi1.order_id) AS times_bought_together
    FROM order_items oi1
    JOIN order_items oi2 ON oi1.order_id = oi2.order_id 
        AND oi1.product_id < oi2.product_id
    JOIN products p1 ON oi1.product_id = p1.product_id
    JOIN products p2 ON oi2.product_id = p2.product_id
    GROUP BY oi1.product_id, oi2.product_id, p1.product_name, p2.product_name
),
product_popularity AS (
    SELECT 
        product_id,
        COUNT(DISTINCT order_id) AS total_orders
    FROM order_items
    GROUP BY product_id
),
affinity_scores AS (
    SELECT 
        pp.product_a,
        pp.product_b,
        pp.product_a_name,
        pp.product_b_name,
        pp.times_bought_together,
        pa.total_orders AS product_a_orders,
        pb.total_orders AS product_b_orders,
        ROUND((pp.times_bought_together * 100.0 / pa.total_orders), 2) AS affinity_score_a,
        ROUND((pp.times_bought_together * 100.0 / pb.total_orders), 2) AS affinity_score_b
    FROM product_pairs pp
    JOIN product_popularity pa ON pp.product_a = pa.product_id
    JOIN product_popularity pb ON pp.product_b = pb.product_id
)
SELECT 
    product_a_name,
    product_b_name,
    times_bought_together,
    affinity_score_a AS product_a_affinity_pct,
    affinity_score_b AS product_b_affinity_pct,
    CASE 
        WHEN affinity_score_a > 20 OR affinity_score_b > 20 THEN 'High Affinity'
        WHEN affinity_score_a > 10 OR affinity_score_b > 10 THEN 'Medium Affinity'
        ELSE 'Low Affinity'
    END AS affinity_level
FROM affinity_scores
ORDER BY times_bought_together DESC, affinity_score_a DESC
LIMIT 15;

-- =============================================
-- CHURN PREDICTION ANALYSIS
-- Identify At-Risk Customers
-- =============================================

WITH customer_activity AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.signup_date,
        c.acquisition_channel,
        MAX(o.order_timestamp) AS last_order_date,
        COUNT(o.order_id) AS total_orders,
        SUM(o.total_amount) AS total_spent,
        DATEDIFF('2024-04-30', MAX(o.order_timestamp)) AS days_since_last_order,
        -- Behavioral features for churn prediction
        AVG(o.total_amount) AS avg_order_value,
        COUNT(DISTINCT DATE_FORMAT(o.order_timestamp, '%Y-%m')) AS active_months
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name, c.signup_date, c.acquisition_channel
),
churn_scores AS (
    SELECT *,
        CASE 
            WHEN days_since_last_order > 90 THEN 'High Risk'
            WHEN days_since_last_order > 60 THEN 'Medium Risk'
            WHEN days_since_last_order > 30 THEN 'Low Risk'
            ELSE 'Active'
        END AS churn_risk,
        CASE 
            WHEN total_orders = 0 THEN 'Never Purchased'
            WHEN total_orders = 1 THEN 'One-Time Buyer'
            WHEN total_orders BETWEEN 2 AND 5 THEN 'Repeat Buyer'
            ELSE 'VIP Customer'
        END AS customer_type
    FROM customer_activity
)
SELECT 
    churn_risk,
    customer_type,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spent), 2) AS avg_lifetime_value,
    ROUND(AVG(days_since_last_order), 2) AS avg_days_inactive,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM churn_scores)), 2) AS percentage_of_total
FROM churn_scores
GROUP BY churn_risk, customer_type
ORDER BY 
    CASE churn_risk
        WHEN 'High Risk' THEN 1
        WHEN 'Medium Risk' THEN 2
        WHEN 'Low Risk' THEN 3
        ELSE 4
    END,
    customer_count DESC;

-- =============================================
-- CUSTOMER SEGMENTATION PYRAMID
-- Advanced Segmentation for Marketing
-- =============================================

WITH customer_segmentation AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.country,
        c.acquisition_channel,
        COUNT(o.order_id) AS order_count,
        COALESCE(SUM(o.total_amount), 0) AS total_spent,
        COUNT(DISTINCT p.session_id) AS total_sessions,
        MAX(o.order_timestamp) AS last_purchase_date,
        -- Engagement Score
        ROUND(
            (COUNT(o.order_id) * 0.4) + 
            (COALESCE(SUM(o.total_amount), 0) / 100 * 0.3) + 
            (COUNT(DISTINCT p.session_id) * 0.3), 
        2) AS engagement_score
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN page_events p ON c.customer_id = p.customer_id
    GROUP BY c.customer_id, c.customer_name, c.country, c.acquisition_channel
),
segmentation_tiers AS (
    SELECT *,
        CASE 
            WHEN engagement_score >= 8 THEN 'Platinum'
            WHEN engagement_score >= 6 THEN 'Gold'
            WHEN engagement_score >= 4 THEN 'Silver'
            WHEN engagement_score >= 2 THEN 'Bronze'
            ELSE 'Lead'
        END AS customer_tier,
        NTILE(4) OVER (ORDER BY total_spent DESC) AS spending_quartile,
        NTILE(4) OVER (ORDER BY order_count DESC) AS frequency_quartile
    FROM customer_segmentation
)
SELECT 
    customer_tier,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spent), 2) AS avg_spending,
    ROUND(AVG(order_count), 2) AS avg_orders,
    ROUND(AVG(engagement_score), 2) AS avg_engagement,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM segmentation_tiers)), 2) AS segment_percentage,
    SUM(total_spent) AS segment_total_revenue
FROM segmentation_tiers
GROUP BY customer_tier
ORDER BY 
    CASE customer_tier
        WHEN 'Platinum' THEN 1
        WHEN 'Gold' THEN 2
        WHEN 'Silver' THEN 3
        WHEN 'Bronze' THEN 4
        ELSE 5
    END;

-- =============================================
-- REAL-TIME BUSINESS INTELLIGENCE DASHBOARD
-- Executive Summary with Key Metrics
-- =============================================

WITH kpi_calculations AS (
    SELECT 
        -- Customer Metrics
        COUNT(DISTINCT c.customer_id) AS total_customers,
        COUNT(DISTINCT o.customer_id) AS purchasing_customers,
        
        -- Order Metrics
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total_amount) AS total_revenue,
        AVG(o.total_amount) AS avg_order_value,
        
        -- Conversion Metrics
        COUNT(DISTINCT p.session_id) AS total_sessions,
        COUNT(DISTINCT o.order_id) AS converted_sessions,
        
        -- Date-based Metrics (Last 30 days)
        SUM(CASE WHEN o.order_timestamp >= DATE_SUB('2024-04-30', INTERVAL 30 DAY) 
                 THEN o.total_amount ELSE 0 END) AS revenue_30d,
        COUNT(CASE WHEN o.order_timestamp >= DATE_SUB('2024-04-30', INTERVAL 30 DAY) 
                   THEN o.order_id END) AS orders_30d
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN page_events p ON c.customer_id = p.customer_id
)
SELECT 
    'Total Customers' AS metric,
    total_customers AS value
FROM kpi_calculations
UNION ALL
SELECT 
    'Purchasing Customers',
    purchasing_customers
FROM kpi_calculations
UNION ALL
SELECT 
    'Total Revenue ($)',
    ROUND(total_revenue, 2)
FROM kpi_calculations
UNION ALL
SELECT 
    'Average Order Value ($)',
    ROUND(avg_order_value, 2)
FROM kpi_calculations
UNION ALL
SELECT 
    'Conversion Rate (%)',
    ROUND((converted_sessions * 100.0 / NULLIF(total_sessions, 0)), 2)
FROM kpi_calculations
UNION ALL
SELECT 
    '30-Day Revenue ($)',
    ROUND(revenue_30d, 2)
FROM kpi_calculations
UNION ALL
SELECT 
    'Customer Acquisition Cost (Est.)',
    ROUND(total_revenue * 0.15 / total_customers, 2)
FROM kpi_calculations;

-- =============================================
-- PROJECT COMPLETION
-- =============================================

SELECT 'ENTERPRISE CUSTOMER ANALYTICS PROJECT COMPLETED SUCCESSFULLY!' AS project_status;
