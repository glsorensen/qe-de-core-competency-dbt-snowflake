{% macro calculate_days_of_supply(inventory_qty, avg_daily_sales) %}
{#
    Calculate days of supply based on current inventory and average daily sales.

    Parameters:
    - inventory_qty: Current inventory quantity
    - avg_daily_sales: Average daily sales units

    Returns:
    - Number of days the inventory will last at current sales velocity
    - NULL if avg_daily_sales is 0 or NULL
#}
    CASE
        WHEN {{ avg_daily_sales }} > 0 THEN
            ROUND({{ inventory_qty }} / {{ avg_daily_sales }}, 1)
        ELSE NULL
    END
{% endmacro %}
