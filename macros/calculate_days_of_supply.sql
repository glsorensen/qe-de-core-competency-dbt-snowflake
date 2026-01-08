{% macro calculate_days_of_supply(inventory_qty, avg_daily_sales) %}
{#
    Calculate Days of Supply
    ========================
    Calculates how many days the current inventory will last based on average daily sales.
    
    Args:
        inventory_qty: Current available inventory quantity
        avg_daily_sales: Average units sold per day
    
    Returns:
        Number of days until stockout, rounded to 1 decimal place
        Returns NULL if avg_daily_sales is 0 or NULL (avoids division by zero)
    
    Example:
        {{ calculate_days_of_supply('quantity_available', 'avg_daily_sales') }}
#}
    CASE
        WHEN {{ avg_daily_sales }} > 0 THEN
            ROUND({{ inventory_qty }} / NULLIF({{ avg_daily_sales }}, 0), 1)
        ELSE NULL
    END
{% endmacro %}
