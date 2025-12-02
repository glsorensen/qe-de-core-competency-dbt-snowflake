{% macro cents_to_dollars(column_name, decimal_places=2) %}
    {#
    ============================================================================
    MACRO: cents_to_dollars
    ============================================================================
    Converts a value in cents to dollars with specified decimal precision.

    This is an example of a reusable macro that demonstrates the DRY principle
    (Don't Repeat Yourself) in dbt projects.

    Usage:
        {{ cents_to_dollars('amount_in_cents') }}
        {{ cents_to_dollars('price_cents', 4) }}

    Args:
        column_name (str): The name of the column containing cents
        decimal_places (int): Number of decimal places (default: 2)

    Returns:
        SQL expression that converts cents to dollars

    Example:
        -- Input: 12345 cents
        -- Output: 123.45 dollars
    ============================================================================
    #}

    ROUND({{ column_name }} / 100.0, {{ decimal_places }})

{% endmacro %}
