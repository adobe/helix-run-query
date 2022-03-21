--- description: Get popularity data for RUM source attribute values, filtered by checkpoint
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 30
--- offset: 0
--- url: 
--- checkpoint: -

WITH 
current_data AS (
  SELECT 
    TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(CAST(time AS INT64)), DAY) AS date,
     * 
  FROM `helix-225321.helix_rum.rum*`
  WHERE 
    # use date partitioning to reduce query size
    _TABLE_SUFFIX <= CONCAT(CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String), LPAD(CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, "0")) AND
    _TABLE_SUFFIX >= CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS String), LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS String), 2, "0")) AND
    CAST(time AS STRING) > CAST(UNIX_MICROS(TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL SAFE_ADD(CAST(@interval AS INT64), -1) DAY)) AS STRING) AND
    CAST(time AS STRING) < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL SAFE_ADD(CAST(@offset AS INT64), 0) DAY)) AS STRING) AND
    url LIKE CONCAT("https://", @url, "%")
),
sources AS (
 SELECT 
    id,
    source, 
    checkpoint,
    MAX(url) AS url, 
    MAX(weight) AS views,
    SUM(weight) AS actions,
  FROM current_data 
  WHERE source IS NOT NULL AND (@checkpoint = '-' OR @checkpoint = checkpoint)
  GROUP BY source, id, checkpoint 
),
groupedsources AS (SELECT 
  COUNT(id) AS ids,
  COUNT(DISTINCT url) AS pages,
  APPROX_TOP_COUNT(url, 1)[OFFSET(0)].value AS topurl,
  SUM(views) AS views,
  SUM(actions) AS actions,
  SUM(actions) / SUM(views) AS actions_per_view,
  checkpoint,
  source,
FROM sources
GROUP BY source, checkpoint
ORDER BY views DESC)
SELECT *,
  CAST((100 * PERCENT_RANK() OVER (PARTITION BY checkpoint ORDER BY views ASC)) AS INT64) AS percentile
FROM groupedsources
ORDER BY views DESC
LIMIT @limit