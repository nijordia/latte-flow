-- Time series: Daily revenue by product (clean legend)
SELECT 
    s.date AS time,                     -- Grafana uses this as time axis
    p.product_name AS Product,        -- This becomes the exact legend/series name
    SUM(s.total_paid)  " "     -- Value field (no prefix needed in legend)
FROM fact_sales s
JOIN dim_product p ON s.product_id = p.product_id
GROUP BY s.date, p.product_name
ORDER BY s.date ASC;

-- Weekly sales by branch
SELECT
    DATE_TRUNC('week', fs.date)::timestamp AS time, -- Cast to timestamp
    db.location_name AS metric,                     -- Use 'metric' for series names
    SUM(fs.total_paid) AS value                     -- The actual data point
FROM fact_sales fs
JOIN dim_branch db ON fs.branch_id = db.branch_id
GROUP BY 1, 2
ORDER BY 1 ASC;

-- Top theft incidents 
SELECT
  s.date_sent AS time,
  db.location_name AS branch_name,
  s.ingredient_id,
  s.sent_qty,
  s.confirmed_qty,
  s.sent_qty - confirmed_qty AS stolen,
  ROUND(((s.sent_qty - s.confirmed_qty) / NULLIF(s.sent_qty, 0) * 100)::numeric, 1) AS theft_pct
FROM shipment s
JOIN dim_branch db ON s.branch_id = db.branch_id
WHERE status = 'confirmed'
  AND sent_qty > 0
ORDER BY theft_pct DESC
LIMIT 20


-- Supplier Orders - Pending and Delivered
SELECT
  order_date AS time,
  ingredient_id,
  order_qty,
  expected_arrival,
  status,
  delivered_at
FROM supplier_orders
ORDER BY order_date DESC;