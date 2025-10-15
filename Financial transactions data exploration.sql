/*
FINANCIAL TRANSACTIONS DATA EXPLORATION 
-- Skills used: CREATE, INSERT, SELECT, UPDATE, DELETE, CTE, Window Functions, 
-- Aggregate Functions, Joins, Subqueries, CASE Statements, Date Functions,
-- String Functions, Mathematical Functions, Views, Conditional Logic
*/

-- =============================================
-- CUSTOMER BEHAVIOR ANALYSIS
-- =============================================

-- Customer Lifetime Value (CLV) Analysis
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.income_bracket,
        COUNT(t.transaction_id) AS total_transactions,
        SUM(t.amount) AS total_spent,
        AVG(t.amount) AS avg_transaction_value,
        MAX(t.transaction_date) AS last_transaction_date,
        MIN(t.transaction_date) AS first_transaction_date,
        DATEDIFF(MAX(t.transaction_date), MIN(t.transaction_date)) AS customer_lifetime_days
    FROM customers c
    JOIN financial_transactions t ON c.customer_id = t.customer_id
    WHERE t.transaction_status = 'completed'
    GROUP BY c.customer_id, c.customer_name, c.income_bracket
),
clv_calculation AS (
    SELECT *,
        total_spent / NULLIF(customer_lifetime_days, 0) * 365 AS estimated_annual_clv,
        NTILE(4) OVER (ORDER BY total_spent DESC) AS customer_value_segment
    FROM customer_metrics
)
SELECT 
    customer_value_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spent), 2) AS avg_total_spent,
    ROUND(AVG(estimated_annual_clv), 2) AS avg_annual_clv,
    ROUND(AVG(total_transactions), 2) AS avg_transactions
FROM clv_calculation
GROUP BY customer_value_segment
ORDER BY customer_value_segment;

-- =============================================
-- ADVANCED TIME SERIES ANALYSIS
-- =============================================

-- Monthly Transaction Trends with Rolling Averages
WITH monthly_metrics AS (
    SELECT 
        YEAR(transaction_date) AS year,
        MONTH(transaction_date) AS month,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_volume,
        AVG(amount) AS avg_transaction_size,
        COUNT(DISTINCT customer_id) AS active_customers,
        SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) AS fraud_cases
    FROM financial_transactions
    WHERE transaction_status = 'completed'
    GROUP BY YEAR(transaction_date), MONTH(transaction_date)
),
rolling_metrics AS (
    SELECT *,
        AVG(total_volume) OVER (
            ORDER BY year, month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg_3month,
        LAG(total_volume, 1) OVER (ORDER BY year, month) AS volume_previous_month
    FROM monthly_metrics
)
SELECT 
    year,
    month,
    transaction_count,
    total_volume,
    ROUND(moving_avg_3month, 2) AS moving_avg_3month,
    volume_previous_month,
    ROUND(((total_volume - volume_previous_month) / volume_previous_month) * 100, 2) AS mom_growth_percentage,
    ROUND((fraud_cases * 100.0 / transaction_count), 4) AS fraud_rate_percentage
FROM rolling_metrics
ORDER BY year, month;

-- =============================================
-- FRAUD DETECTION & ANOMALY DETECTION
-- =============================================

-- Statistical Anomaly Detection using Z-Scores
WITH customer_spending_patterns AS (
    SELECT 
        customer_id,
        AVG(amount) AS avg_amount,
        STDDEV(amount) AS std_amount,
        COUNT(*) AS transaction_count
    FROM financial_transactions
    WHERE transaction_status = 'completed' 
      AND transaction_type = 'purchase'
    GROUP BY customer_id
    HAVING COUNT(*) >= 3
),
transaction_anomalies AS (
    SELECT 
        t.transaction_id,
        t.customer_id,
        c.customer_name,
        t.amount,
        t.transaction_date,
        t.merchant_category,
        cp.avg_amount,
        cp.std_amount,
        (t.amount - cp.avg_amount) / NULLIF(cp.std_amount, 0) AS z_score,
        CASE 
            WHEN ABS((t.amount - cp.avg_amount) / NULLIF(cp.std_amount, 0)) > 3 THEN 'High Anomaly'
            WHEN ABS((t.amount - cp.avg_amount) / NULLIF(cp.std_amount, 0)) > 2 THEN 'Medium Anomaly'
            ELSE 'Normal'
        END AS anomaly_level
    FROM financial_transactions t
    JOIN customer_spending_patterns cp ON t.customer_id = cp.customer_id
    JOIN customers c ON t.customer_id = c.customer_id
    WHERE t.transaction_status = 'completed'
)
SELECT 
    anomaly_level,
    COUNT(*) AS transaction_count,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_capture_rate
FROM transaction_anomalies
GROUP BY anomaly_level
ORDER BY 
    CASE anomaly_level
        WHEN 'High Anomaly' THEN 1
        WHEN 'Medium Anomaly' THEN 2
        ELSE 3
    END;

-- =============================================
-- RFM ANALYSIS (Recency, Frequency, Monetary)
-- =============================================

-- Customer Segmentation using RFM
WITH rfm_calculation AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.income_bracket,
        DATEDIFF('2024-02-01', MAX(t.transaction_date)) AS recency_days,
        COUNT(t.transaction_id) AS frequency,
        SUM(t.amount) AS monetary,
        -- RFM Scoring (1-5, with 5 being best)
        NTILE(5) OVER (ORDER BY DATEDIFF('2024-02-01', MAX(t.transaction_date)) DESC) AS recency_score,
        NTILE(5) OVER (ORDER BY COUNT(t.transaction_id)) AS frequency_score,
        NTILE(5) OVER (ORDER BY SUM(t.amount)) AS monetary_score
    FROM customers c
    JOIN financial_transactions t ON c.customer_id = t.customer_id
    WHERE t.transaction_status = 'completed'
    GROUP BY c.customer_id, c.customer_name, c.income_bracket
),
rfm_segments AS (
    SELECT *,
        CONCAT(recency_score, frequency_score, monetary_score) AS rfm_cell,
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 THEN 'New Customers'
            WHEN frequency_score >= 4 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost Customers'
            ELSE 'Need Attention'
        END AS customer_segment
    FROM rfm_calculation
)
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(monetary), 2) AS avg_spending,
    ROUND(AVG(frequency), 2) AS avg_frequency,
    ROUND(AVG(recency_days), 2) AS avg_recency_days
FROM rfm_segments
GROUP BY customer_segment
ORDER BY avg_spending DESC;

-- =============================================
-- MERCHANT & CATEGORY ANALYSIS
-- =============================================

-- Merchant Category Performance
SELECT 
    merchant_category,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_volume,
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_percent,
    ROUND(SUM(amount) / COUNT(DISTINCT customer_id), 2) AS revenue_per_customer
FROM financial_transactions
WHERE transaction_status = 'completed'
GROUP BY merchant_category
ORDER BY total_volume DESC;

-- =============================================
-- 6. GEOGRAPHICAL ANALYSIS
-- =============================================

-- Regional Performance Analysis
SELECT 
    location_city,
    location_country,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_volume,
    ROUND(AVG(amount), 2) AS avg_transaction_size,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS mobile_usage_percent,
    ROUND(SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_percent
FROM financial_transactions
WHERE transaction_status = 'completed'
GROUP BY location_city, location_country
ORDER BY total_volume DESC;

-- =============================================
-- DEVICE & CHANNEL ANALYSIS
-- =============================================

-- Multi-dimensional Channel Performance
SELECT 
    device_type,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_volume,
    ROUND(AVG(amount), 2) AS avg_transaction_size,
    ROUND(SUM(CASE WHEN transaction_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS purchase_rate_percent,
    ROUND(SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_percent,
    -- Most common transaction hours for each device
    CASE 
        WHEN HOUR(MIN(transaction_date)) <= 12 THEN 'Morning'
        WHEN HOUR(MIN(transaction_date)) <= 18 THEN 'Afternoon'
        ELSE 'Evening'
    END AS peak_usage_time
FROM financial_transactions
WHERE transaction_status = 'completed'
GROUP BY device_type
ORDER BY total_volume DESC;

-- =============================================
-- TRANSACTION PATTERN ANALYSIS
-- =============================================

-- Customer Transaction Sequences
WITH transaction_sequences AS (
    SELECT 
        customer_id,
        transaction_date,
        transaction_type,
        amount,
        merchant_category,
        LAG(transaction_type) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS previous_transaction_type,
        LAG(amount) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS previous_amount,
        LEAD(transaction_type) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS next_transaction_type,
        DATEDIFF(transaction_date, LAG(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date)) AS days_since_last_transaction
    FROM financial_transactions
    WHERE transaction_status = 'completed'
),
sequence_patterns AS (
    SELECT 
        CONCAT(previous_transaction_type, ' → ', transaction_type, ' → ', next_transaction_type) AS transaction_sequence,
        COUNT(*) AS total_occurrences,
        ROUND(AVG(amount), 2) AS avg_transaction_amount,
        ROUND(AVG(days_since_last_transaction), 2) AS avg_days_between
    FROM transaction_sequences
    WHERE previous_transaction_type IS NOT NULL AND next_transaction_type IS NOT NULL
    GROUP BY transaction_sequence
)
SELECT 
    transaction_sequence,
    total_occurrences,
    avg_transaction_amount,
    avg_days_between
FROM sequence_patterns
WHERE total_occurrences >= 2
ORDER BY total_occurrences DESC;

-- =============================================
-- FRAUD ANALYSIS DEEP DIVE
-- =============================================

-- Comprehensive Fraud Analysis
SELECT 
    -- Time Analysis
    HOUR(transaction_date) AS transaction_hour,
    DAYNAME(transaction_date) AS transaction_day,
    
    -- Transaction Characteristics
    transaction_type,
    merchant_category,
    device_type,
    
    -- Location Analysis
    location_city,
    
    -- Statistical Analysis
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) AS fraud_cases,
    ROUND(SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_percent,
    
    -- Amount Analysis
    ROUND(AVG(CASE WHEN is_fraudulent = 1 THEN amount END), 2) AS avg_fraud_amount,
    ROUND(AVG(CASE WHEN is_fraudulent = 0 THEN amount END), 2) AS avg_legit_amount
    
FROM financial_transactions
WHERE transaction_status = 'completed'
GROUP BY 
    HOUR(transaction_date), 
    DAYNAME(transaction_date),
    transaction_type,
    merchant_category,
    device_type,
    location_city
HAVING fraud_cases > 0
ORDER BY fraud_rate_percent DESC;

-- =============================================
-- CUSTOMER SEGMENTATION SUMMARY
-- =============================================

-- Comprehensive Customer Segmentation
WITH customer_summary AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.income_bracket,
        c.credit_score,
        COUNT(t.transaction_id) AS total_transactions,
        SUM(t.amount) AS total_spent,
        AVG(t.amount) AS avg_transaction_value,
        MAX(t.transaction_date) AS last_transaction,
        COUNT(DISTINCT t.merchant_category) AS unique_categories,
        SUM(CASE WHEN t.is_fraudulent = 1 THEN 1 ELSE 0 END) AS fraud_attempts,
        a.balance AS current_balance
    FROM customers c
    LEFT JOIN financial_transactions t ON c.customer_id = t.customer_id
    LEFT JOIN accounts a ON c.customer_id = a.customer_id AND a.account_type = 'checking'
    WHERE t.transaction_status = 'completed' OR t.transaction_id IS NULL
    GROUP BY c.customer_id, c.customer_name, c.income_bracket, c.credit_score, a.balance
)
SELECT 
    income_bracket,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spent), 2) AS avg_total_spent,
    ROUND(AVG(credit_score), 2) AS avg_credit_score,
    ROUND(AVG(current_balance), 2) AS avg_balance,
    ROUND(AVG(total_transactions), 2) AS avg_transactions,
    ROUND(SUM(fraud_attempts) * 100.0 / SUM(total_transactions), 4) AS overall_fraud_rate
FROM customer_summary
GROUP BY income_bracket
ORDER BY avg_total_spent DESC;

-- =============================================
-- BONUS: REAL-TIME BUSINESS METRICS DASHBOARD
-- =============================================

-- Key Performance Indicators (KPIs)
SELECT 
    -- Volume Metrics
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_volume,
    COUNT(DISTINCT customer_id) AS active_customers,
    
    -- Average Metrics
    ROUND(AVG(amount), 2) AS avg_transaction_size,
    
    -- Fraud Metrics
    SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) AS fraud_cases,
    ROUND(SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate,
    
    -- Success Rate
    ROUND(SUM(CASE WHEN transaction_status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate,
    
    -- Device Distribution
    ROUND(SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS mobile_rate
    
FROM financial_transactions
WHERE transaction_date >= '2024-01-01';

-- =============================================
-- VIEWS FOR REPORTING
-- =============================================

-- Create View for Customer Analytics
CREATE VIEW customer_analytics AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.income_bracket,
    c.credit_score,
    COUNT(t.transaction_id) AS transaction_count,
    SUM(t.amount) AS total_spent,
    AVG(t.amount) AS avg_transaction_value,
    MAX(t.transaction_date) AS last_transaction_date,
    COUNT(DISTINCT t.merchant_category) AS diversity_score
FROM customers c
LEFT JOIN financial_transactions t ON c.customer_id = t.customer_id
WHERE t.transaction_status = 'completed'
GROUP BY c.customer_id, c.customer_name, c.income_bracket, c.credit_score;

-- Create View for Fraud Monitoring
CREATE VIEW fraud_monitoring AS
SELECT 
    transaction_date,
    customer_id,
    amount,
    merchant_category,
    device_type,
    location_city,
    CASE 
        WHEN amount > 1000 THEN 'High Value'
        WHEN amount > 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS value_category
FROM financial_transactions
WHERE is_fraudulent = 1
ORDER BY transaction_date DESC;

-- =============================================
-- FINAL SUMMARY QUERY
-- =============================================

-- Executive Summary
SELECT 
    'Total Customers' AS metric,
    COUNT(*) AS value
FROM customers
UNION ALL
SELECT 
    'Total Transactions',
    COUNT(*)
FROM financial_transactions
WHERE transaction_status = 'completed'
UNION ALL
SELECT 
    'Total Volume ($)',
    ROUND(SUM(amount), 2)
FROM financial_transactions
WHERE transaction_status = 'completed'
UNION ALL
SELECT 
    'Average Transaction ($)',
    ROUND(AVG(amount), 2)
FROM financial_transactions
WHERE transaction_status = 'completed'
UNION ALL
SELECT 
    'Fraud Rate (%)',
    ROUND(SUM(CASE WHEN is_fraudulent = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4)
FROM financial_transactions
WHERE transaction_status = 'completed'
UNION ALL
SELECT 
    'Mobile Usage (%)',
    ROUND(SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM financial_transactions
WHERE transaction_status = 'completed';

-- Show created views
SELECT 'Customer Analytics View Created' AS status
FROM information_schema.views 
WHERE table_name = 'customer_analytics'
UNION ALL
SELECT 'Fraud Monitoring View Created' AS status
FROM information_schema.views 
WHERE table_name = 'fraud_monitoring';

-- =============================================
-- PROJECT COMPLETION MESSAGE
-- =============================================

SELECT 'FINANCIAL TRANSACTIONS ANALYSIS PROJECT COMPLETED SUCCESSFULLY!' AS project_status;
