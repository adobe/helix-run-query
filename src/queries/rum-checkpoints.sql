--- description: Get RUM data by checkpoint to see which checkpoint causes the greatest dropoff in traffic
--- Authorization: none
--- interval: 30
--- domain: -
--- generation: -
--- device: all

CREATE TEMP FUNCTION FILTERCLASS(user_agent STRING, device STRING) 
  RETURNS BOOLEAN
  AS (
    device = "all" OR 
    (device = "desktop" AND user_agent NOT LIKE "%Mobile%" AND user_agent LIKE "Mozilla%" ) OR 
    (device = "mobile" AND user_agent LIKE "%Mobile%") OR
    (device = "bot" AND user_agent NOT LIKE "Mozilla%"));

WITH rootdata AS (
    SELECT 
        checkpoint, 
        id, 
        url, 
        @generation AS generation,
        weight 
    FROM `helix-225321.helix_rum.rum*`
    WHERE 
      # use date partitioning to reduce query size
      _TABLE_SUFFIX <= CONCAT(CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String), LPAD(CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, "0")) AND
      _TABLE_SUFFIX >= CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS String), LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS String), 2, "0")) AND
      CAST(time AS STRING) > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS STRING) AND
      CAST(time AS STRING) < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 DAY)) AS STRING) AND
      (generation = @generation OR @generation = "-") AND
      (url LIKE CONCAT("https://", @domain, "%") OR @domain = "-") AND
      FILTERCLASS(user_agent, @device)
),
weightdata AS (
    SELECT
        checkpoint,
        MAX(weight) AS weight,
        id,
        MAX(url) AS url,
        MAX(generation) AS generation,
    FROM rootdata
    GROUP BY
        id,
        checkpoint
),
data AS (
SELECT 
    checkpoint, 
    COUNT(DISTINCT id) AS ids,
    SUM(weight) AS views,
    # url
FROM weightdata 
WHERE 
    checkpoint IS NOT NULL
GROUP BY 
    checkpoint, 
    generation 
    #, url
ORDER BY ids DESC),
anydatabyid AS (
    SELECT 
        "any" AS checkpoint,
        COUNT(DISTINCT id) AS ids,
        MAX(weight) AS views,
        # url
    FROM weightdata 
    WHERE 
        checkpoint IS NOT NULL #IN ("top", "unsupported", "noscript")
    GROUP BY
        id
    ORDER BY ids DESC
),
anydata AS (
    SELECT
        MIN(checkpoint) AS checkpoint,
        COUNT(DISTINCT ids) AS ids,
        SUM(views) AS views,
        #, url
    FROM anydatabyid 
),
alldata AS (
    SELECT * FROM (SELECT * FROM anydata UNION ALL (SELECT * FROM data)))
SELECT 
    checkpoint, 
    ids as events,
    views,
    IF(MAX(views) OVER(
        ORDER BY views DESC
        ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING  
    ) != 0, ROUND(100 - (100 * views / MAX(views) OVER(
        ORDER BY views DESC
        ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING  
    )), 1), 0) AS percent_dropoff,
    IF(MAX(views) OVER (
        ORDER BY  views DESC) != 0, ROUND(100 * views / MAX(views) OVER (
        ORDER BY  views DESC), 1), 0) AS percent_total

FROM alldata