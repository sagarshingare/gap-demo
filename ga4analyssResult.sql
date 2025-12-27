EXPORT DATA OPTIONS (
  uri = 'gs://ga4-dataform/top_traffic_source_medium_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT *
FROM gold.top_traffic_source_medium order by 1,2;


select distinct traffic_source_medium from `silver.purchase_traffic_source_medium`;

select * from `silver.purchase_traffic_source_medium`
WHERE
  event_date IS NULL
  OR traffic_source_medium IS NULL or lower(traffic_source_medium)="(none)"
  OR event_value_in_usd < 0;

SELECT
  traffic_source_medium,
  SUM(total_purchase_value_usd) AS lifetime_value,
  SUM(total_purchases) AS lifetime_purchases
FROM `gold.top_traffic_source_medium`
GROUP BY traffic_source_medium
ORDER BY lifetime_value DESC;


SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY month
           ORDER BY total_purchase_value_usd DESC
         ) AS rank
  FROM `gold.top_traffic_source_medium`
)
WHERE rank = 1;




