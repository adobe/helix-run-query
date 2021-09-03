--- description: Get daily page views for a site according to Helix RUM data
--- Authorization: none
--- limit: 30
--- offset: 0
--- url: 

WITH 
current_data AS (
  SELECT 
    TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(CAST(time AS INT64)), DAY) AS date,
     * 
  FROM `helix-225321.helix_rum.rum*`
  WHERE 
    # use date partitioning to reduce query size
    _TABLE_SUFFIX <= CONCAT(CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String), LPAD(CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, "0")) AND
    _TABLE_SUFFIX >= CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@limit AS INT64) DAY)) AS String), LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@limit AS INT64) DAY)) AS String), 2, "0")) AND
    CAST(time AS STRING) > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@limit AS INT64) DAY)) AS STRING) AND
    CAST(time AS STRING) < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@offset AS INT64) DAY)) AS STRING) AND
    url LIKE CONCAT("https://", @url, "%")
),
pageviews_by_id AS (
    SELECT 
        MAX(date) AS date, 
        MAX(url) AS url,
        id,
        MAX(weight) AS weight
    FROM current_data 
    GROUP BY id)
SELECT
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    STRING(date) AS time,
    COUNT(url) AS urls,
    SUM(weight) AS pageviews,
FROM pageviews_by_id
GROUP BY date
ORDER BY date DESC