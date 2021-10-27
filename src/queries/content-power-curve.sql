--- description: Show content reach and persistence over time
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 30
--- interval: 60
--- offset: 0
--- domain: -
DECLARE upperdate STRING DEFAULT CONCAT(
  CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@offset AS INT64) DAY)) AS String), 
  LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(), 
    INTERVAL CAST(@offset AS INT64) DAY)) AS String), 2, "0"));

DECLARE lowerdate STRING DEFAULT CONCAT(
  CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL SAFE_ADD(CAST(@interval AS INT64), CAST(@offset AS INT64)) DAY)) AS String), 
  LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(
    CURRENT_TIMESTAMP(), 
    INTERVAL SAFE_ADD(CAST(@interval AS INT64), CAST(@offset AS INT64)) DAY)) AS String), 2, "0"));

DECLARE uppertimestamp STRING DEFAULT CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@offset AS INT64) DAY)) AS STRING);

DECLARE lowertimestamp STRING DEFAULT CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL SAFE_ADD(CAST(@interval AS INT64), CAST(@offset AS INT64)) DAY)) AS STRING);

WITH visits AS (
  SELECT 
    MAX(url) AS url, 
    TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(CAST(MAX(time) AS INT64)), DAY) AS time, 
    id, 
    MAX(weight) AS weight 
  FROM `helix-225321.helix_rum.rum*` 
  WHERE 
    # use date partitioning to reduce query size
    _TABLE_SUFFIX <= upperdate AND
    _TABLE_SUFFIX >= lowerdate AND
    CAST(time AS STRING) < uppertimestamp AND
    CAST(time AS STRING) > lowertimestamp AND
    (@domain = "-" OR url LIKE CONCAT("https://", @domain, "%"))
  GROUP BY id
),
urldays AS (
  SELECT time, url, COUNT(id) AS events, SUM(weight) AS visits FROM visits # FULL JOIN days ON (days.time = visits.time)
  GROUP BY time, url
),
steps AS (
  SELECT time, url, events, visits, 
  TIMESTAMP_DIFF(time, LAG(time) OVER(PARTITION BY url ORDER BY time), DAY) AS step 
  FROM urldays
),
chains AS (
  SELECT time, url, events, visits, steps, 
      COUNTIF(step = 1) OVER(PARTITION BY url ORDER BY time) AS chain
      FROM steps
),
urlchains AS (
  SELECT url, time, chain, events, visits FROM chains
  ORDER BY chain DESC
),
powercurve AS (
  SELECT 
    MAX(chain) AS persistence, 
    count(url) AS reach
  FROM urlchains
  GROUP BY chain
  ORDER BY MAX(chain) ASC
  LIMIT 31 OFFSET 1
)
SELECT * FROM powercurve