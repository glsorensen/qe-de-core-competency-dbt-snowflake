-- ============================================================================
-- Exercise 1 Verification: Product Categories Analytics
-- ============================================================================
-- Copy each query below into Snowflake to explore your new analytics!
-- ============================================================================

-- Query 1: View all categories in the dimension table
-- Expected: 6 categories (Electronics, Clothing, Home, Sports, Accessories, Health)
SELECT * 
FROM SALES_DATABASE_MEAGAN.gold.dim_categories 
ORDER BY category_id;

-- Query 2: Sales performance by category (full report)
-- Shows all metrics: products, orders, revenue, performance tiers
SELECT 
    category_name,
    department,
    total_products,
    total_brands,
    total_orders,
    total_line_items,
    total_quantity_sold,
    total_revenue,
    total_revenue_after_discount,
    total_discounts_given,
    performance_tier
FROM SALES_DATABASE_MEAGAN.gold.rpt_sales_by_category 
ORDER BY total_revenue DESC;

-- Query 3: Top performing categories only
-- Shows only categories with sales
SELECT 
    category_name,
    department,
    total_products,
    total_orders,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_order_value, 2) AS avg_order_value,
    performance_tier
FROM SALES_DATABASE_MEAGAN.gold.rpt_sales_by_category 
WHERE total_revenue > 0
ORDER BY total_revenue DESC;

-- Query 4: Which products are in each category?
-- Shows the product-category mapping
SELECT 
    c.category_name,
    c.department,
    p.product_name,
    p.brand,
    ROUND(p.price, 2) AS price,
    p.is_active
FROM SALES_DATABASE_MEAGAN.silver.stg_products p
INNER JOIN SALES_DATABASE_MEAGAN.silver.stg_categories c 
    ON p.category_id = c.category_id
ORDER BY c.category_name, p.product_name;

-- Query 5: Category summary with product counts
-- Quick overview of each category
SELECT 
    c.category_name,
    c.department,
    COUNT(p.product_id) AS product_count,
    COUNT(DISTINCT p.brand) AS brand_count,
    ROUND(AVG(p.price), 2) AS avg_product_price,
    ROUND(MIN(p.price), 2) AS min_price,
    ROUND(MAX(p.price), 2) AS max_price
FROM SALES_DATABASE_MEAGAN.silver.stg_categories c
LEFT JOIN SALES_DATABASE_MEAGAN.silver.stg_products p 
    ON c.category_id = p.category_id
GROUP BY c.category_name, c.department
ORDER BY product_count DESC;

-- Query 6: Revenue contribution by department
-- Roll up category sales to department level
SELECT 
    department,
    COUNT(DISTINCT category_id) AS category_count,
    SUM(total_products) AS total_products,
    SUM(total_orders) AS total_orders,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM SALES_DATABASE_MEAGAN.gold.rpt_sales_by_category
GROUP BY department
ORDER BY total_revenue DESC;
