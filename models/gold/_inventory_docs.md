{% docs inventory_health_status %}

### Inventory Health Classification

Inventory health status is determined based on current stock levels relative to the reorder point:

| Status          | Condition                                   | Action Required          |
| --------------- | ------------------------------------------- | ------------------------ |
| **critical**    | Stockout - zero available inventory         | Immediate reorder        |
| **low**         | Below reorder point (stock ≤ reorder_point) | Reorder soon             |
| **healthy**     | Above reorder point but below 3x            | No action needed         |
| **overstocked** | More than 3x the reorder point              | Consider reducing orders |

**Business Impact:**

- **Critical**: Lost sales, customer dissatisfaction, potential backorders
- **Low**: Risk of stockout during lead time
- **Healthy**: Optimal inventory level balancing availability and carrying costs
- **Overstocked**: Excess carrying costs, potential obsolescence risk

{% enddocs %}

{% docs supplier_tier %}

### Supplier Tier Classification

Suppliers are classified into tiers based on their performance and relationship status:

| Tier         | Criteria                                        |
| ------------ | ----------------------------------------------- |
| **Platinum** | Preferred supplier AND reliability score > 0.90 |
| **Gold**     | Preferred supplier OR reliability score > 0.85  |
| **Silver**   | All other suppliers                             |

**Benefits by Tier:**

- **Platinum**: Priority allocation, strategic partnership, joint planning
- **Gold**: Favorable terms, regular business reviews
- **Silver**: Standard terms, transactional relationship

{% enddocs %}

{% docs stockout_risk_score %}

### Stockout Risk Score

A score from 0-100 indicating the risk of stockout, with higher values indicating greater risk:

| Score Range | Risk Level | Description                                     |
| ----------- | ---------- | ----------------------------------------------- |
| 90-100      | Critical   | Stockout imminent or already occurred           |
| 70-89       | High       | Stock will deplete before next delivery arrives |
| 50-69       | Medium     | Below reorder point, needs attention            |
| 10-49       | Low        | Adequate stock levels                           |
| 0-9         | Minimal    | Well-stocked or no recent sales activity        |

**Calculation Factors:**

- Current available inventory
- Average daily sales velocity (30-day lookback)
- Supplier lead time
- Reorder point threshold

{% enddocs %}

{% docs anomaly_types %}

### Inventory Anomaly Types

The system detects the following types of inventory anomalies:

| Anomaly Type               | Detection Rule                                       | Severity |
| -------------------------- | ---------------------------------------------------- | -------- |
| **Negative Inventory**     | Running balance goes below zero (data quality issue) | 5        |
| **Large Transaction**      | Transaction size > 3 standard deviations from mean   | 4-5      |
| **Cost Anomaly**           | Unit cost > 2 standard deviations from average       | 3-4      |
| **Unexplained Adjustment** | Adjustment > 10 units without notes                  | 3        |
| **Weekend Activity**       | Large restock/adjustment on Saturday or Sunday       | 2        |
| **Rapid Depletion**        | Inventory drops > 50% in one week                    | 3        |

**Investigation Priority:**

- Severity 5: Immediate investigation required
- Severity 4: Investigate within 24 hours
- Severity 3: Review within the week
- Severity 2: Monitor for patterns
- Severity 1: Low priority review

{% enddocs %}

{% docs days_of_supply %}

### Days of Supply Calculation

Days of supply indicates how many days the current inventory will last at the current sales rate:

```
Days of Supply = Quantity Available / Average Daily Sales
```

**Interpretation:**

- **< Lead Time**: High stockout risk - order immediately
- **< 1.5x Lead Time**: Elevated risk - consider ordering
- **2-4 weeks**: Optimal range for most products
- **> 8 weeks**: May be overstocked, review demand forecast

**Note:** Products with no recent sales will show NULL for days of supply.

{% enddocs %}

{% docs scd_type_2 %}

### Slowly Changing Dimension Type 2

The `dim_inventory_history` table implements SCD Type 2 to track historical inventory states:

**Key Columns:**

- `valid_from`: Date when this inventory state became effective
- `valid_to`: Date when this state was superseded (9999-12-31 for current)
- `is_current`: Boolean flag indicating the most recent record per product

**Point-in-Time Query Example:**

```sql
SELECT *
FROM dim_inventory_history
WHERE product_id = 1
  AND '2023-08-15' BETWEEN valid_from AND valid_to;
```

**Current State Query Example:**

```sql
SELECT *
FROM dim_inventory_history
WHERE is_current = TRUE;
```

{% enddocs %}
