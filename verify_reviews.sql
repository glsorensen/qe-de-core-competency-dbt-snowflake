-- ============================================================================
-- Exercise 4: Customer Reviews Analytics - Verification Queries
-- ============================================================================
-- Run these queries in Snowflake to explore your review data
-- Make sure to use the correct database and schema (e.g., USE SCHEMA gold;)
-- ============================================================================

-- Query 1: Top Rated Products
-- Shows products with the highest average ratings
SELECT 
    product_name,
    brand,
    total_reviews,
    avg_rating,
    positive_review_pct
FROM gold.rpt_product_ratings
ORDER BY avg_rating DESC, total_reviews DESC
LIMIT 10;

-- Query 2: Products Needing Attention (Low Ratings)
-- Identifies products with poor customer satisfaction
SELECT 
    product_name,
    brand,
    price,
    total_reviews,
    avg_rating,
    min_rating,
    positive_review_pct
FROM gold.rpt_product_ratings
WHERE avg_rating < 3
ORDER BY avg_rating ASC;

-- Query 3: Most Active Reviewers
-- Shows customers who write the most reviews
SELECT 
    first_name,
    last_name,
    email,
    total_reviews_written,
    avg_rating_given,
    reviewer_type,
    days_since_last_review
FROM gold.rpt_customer_review_activity
ORDER BY total_reviews_written DESC
LIMIT 10;

-- Query 4: Harsh Critics
-- Finds customers who tend to give low ratings
SELECT 
    first_name,
    last_name,
    email,
    total_reviews_written,
    avg_rating_given,
    reviewer_type
FROM gold.rpt_customer_review_activity
WHERE reviewer_type = 'Harsh Critic'
ORDER BY avg_rating_given ASC;

-- Query 5: Review Activity Timeline
-- Shows when reviews were posted over time
SELECT 
    DATE_TRUNC('month', review_date) AS review_month,
    COUNT(*) AS total_reviews,
    ROUND(AVG(rating), 2) AS avg_rating,
    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS positive_reviews
FROM gold.fct_reviews
GROUP BY DATE_TRUNC('month', review_date)
ORDER BY review_month DESC;

-- Query 6: Recent Poor Reviews (Needs Response)
-- Shows recent negative reviews that may need customer service follow-up
SELECT 
    r.review_id,
    r.product_id,
    p.product_name,
    c.first_name,
    c.last_name,
    c.email,
    r.rating,
    r.review_title,
    r.review_text,
    r.review_date,
    r.review_age_days
FROM gold.fct_reviews r
LEFT JOIN silver.stg_products p ON r.product_id = p.product_id
LEFT JOIN silver.stg_customers c ON r.customer_id = c.customer_id
WHERE r.rating <= 2
    AND r.review_age_days <= 30
ORDER BY r.review_date DESC;

-- Query 7: Rating Distribution
-- Shows how many reviews at each rating level
SELECT 
    rating,
    rating_category,
    COUNT(*) AS review_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS percentage
FROM gold.fct_reviews
GROUP BY rating, rating_category
ORDER BY rating DESC;

-- Query 8: Products by Category with Review Metrics
-- Combines category and review data for comprehensive analysis
SELECT 
    c.category_name,
    p.product_name,
    p.total_reviews,
    p.avg_rating,
    p.positive_review_pct,
    p.price
FROM gold.rpt_product_ratings p
LEFT JOIN silver.stg_products sp ON p.product_id = sp.product_id
LEFT JOIN silver.stg_categories c ON sp.category_id = c.category_id
ORDER BY c.category_name, p.avg_rating DESC;

-- Query 9: Customer Review Engagement by Segment
-- Analyzes which customer segments are most engaged with reviews
SELECT 
    d.customer_segment,
    COUNT(DISTINCT r.customer_id) AS reviewers,
    COUNT(*) AS total_reviews,
    ROUND(AVG(r.rating), 2) AS avg_rating_given,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT r.customer_id), 2) AS reviews_per_customer
FROM gold.fct_reviews r
LEFT JOIN gold.dim_customers d ON r.customer_id = d.customer_id
GROUP BY d.customer_segment
ORDER BY total_reviews DESC;

-- Query 10: Most Controversial Products (High Rating Variance)
-- Products with the biggest spread between high and low ratings
SELECT 
    product_name,
    total_reviews,
    avg_rating,
    min_rating,
    max_rating,
    (max_rating - min_rating) AS rating_variance
FROM gold.rpt_product_ratings
WHERE total_reviews >= 2
ORDER BY rating_variance DESC, total_reviews DESC;
