--- description: Get RUM data by checkpoint to see which checkpoint causes the greatest dropoff in traffic
--- Authorization: none
--- limit: 10
--- interval: 30
--- domain: -
--- generation: -
--- generationb: -
--- device: all

CREATE TEMP FUNCTION FILTERCLASS(user_agent STRING, device STRING) 
  RETURNS BOOLEAN
  AS (
    device = "all" OR 
    (device = "desktop" AND user_agent NOT LIKE "%Mobile%" AND user_agent LIKE "Mozilla%" ) OR 
    (device = "mobile" AND user_agent LIKE "%Mobile%") OR
    (device = "bot" AND user_agent NOT LIKE "Mozilla%"));

WITH rootdata AS (
    SELECT * FROM `helix-225321.helix_rum.rum202109`
    WHERE 
      # use date partitioning to reduce query size
      _TABLE_SUFFIX <= CONCAT(CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String), LPAD(CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, "0")) AND
      _TABLE_SUFFIX >= CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS String), LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS String), 2, "0")) AND
      CAST(time AS STRING) > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS STRING) AND
      CAST(time AS STRING) < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 DAY)) AS STRING) AND
      (generation = @generation OR @generation = "-") AND
      (url LIKE CONCAT("https://", @domain, "%") OR url = "-") AND
      FILTERCLASS(user_agent, @device)
),
data AS (
SELECT 
    checkpoint, 
    COUNT(DISTINCT id) AS ids
    # url
FROM rootdata 
WHERE 
    checkpoint IS NOT NULL
GROUP BY checkpoint, generation #, url
ORDER BY ids DESC),
anydata AS (
    SELECT 
    "any" AS checkpoint,
    COUNT(DISTINCT id) AS ids
    # url
FROM rootdata 
WHERE 
    checkpoint IS NOT NULL
ORDER BY ids DESC
),
alldata AS (
    SELECT * FROM (SELECT * FROM anydata UNION ALL (SELECT * FROM data)))

SELECT checkpoint, ids as events, 
    ROUND(100 - (100 * ids / MAX(ids) OVER(
        ORDER BY ids DESC
        ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING  
    )), 1) AS percent_dropoff,
    ROUND(100 * ids / MAX(ids) OVER (
        ORDER BY  ids DESC), 1) AS percent_total

FROM alldata