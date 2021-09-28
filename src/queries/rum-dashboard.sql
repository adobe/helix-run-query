--- description: Get Helix RUM data for a given domain or owner/repo combination
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 10
--- interval: 30
--- domain: -
--- owner: -
--- repo: -
--- generationa: -
--- generationb: -
--- device: all

CREATE TEMP FUNCTION FILTERCLASS(user_agent STRING, device STRING) 
  RETURNS BOOLEAN
  AS (
    device = "all" OR 
    (device = "desktop" AND user_agent NOT LIKE "%Mobile%" AND user_agent LIKE "Mozilla%" ) OR 
    (device = "mobile" AND user_agent LIKE "%Mobile%") OR
    (device = "bot" AND user_agent NOT LIKE "Mozilla%"));


WITH 
current_data AS (
  SELECT * 
  FROM `helix-225321.helix_rum.rum*`
  WHERE 
    # use date partitioning to reduce query size
    _TABLE_SUFFIX <= CONCAT(CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String), LPAD(CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, "0")) AND
    _TABLE_SUFFIX >= CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS String), LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS String), 2, "0")) AND
    CAST(time AS STRING) > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS STRING) AND
    CAST(time AS STRING) < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 DAY)) AS STRING) AND
    FILTERCLASS(user_agent, @device) AND
    (generation = @generationa OR @generationa = "-")
),
previous_data AS (
  SELECT * 
  FROM `helix-225321.helix_rum.rum*`
  WHERE 
    # use date partitioning to reduce query size
    ((@generationb = "-" AND
    _TABLE_SUFFIX <= CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS String), LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS String), 2, "0")) AND
    _TABLE_SUFFIX >= CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL (2 * CAST(@interval AS INT64)) DAY)) AS String), LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS String), 2, "0")) AND
    CAST(time AS STRING) > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL (2 * CAST(@interval AS INT64)) DAY)) AS STRING) AND
    CAST(time AS STRING) < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(@interval AS INT64) DAY)) AS STRING)) OR generation = @generationb) AND
    FILTERCLASS(user_agent, @device)
),
current_rum_by_id AS (
    SELECT 
    IF(MAX(LCP) IS NULL, NULL, IF(MAX(LCP) < 2500, TRUE, FALSE)) AS lcpgood,
    IF(MAX(FID) IS NULL, NULL, IF(MAX(FID) < 100, TRUE, FALSE)) AS fidgood,
    IF(MAX(CLS) IS NULL, NULL, IF(MAX(CLS) < 0.1, TRUE, FALSE)) AS clsgood,
    IF(MAX(LCP) IS NULL, NULL, IF(MAX(LCP) > 4000, TRUE, FALSE)) AS lcpbad,
    IF(MAX(FID) IS NULL, NULL, IF(MAX(FID) > 300, TRUE, FALSE)) AS fidbad,
    IF(MAX(CLS) IS NULL, NULL, IF(MAX(CLS) > 0.25, TRUE, FALSE)) AS clsbad,
    MAX(host) AS host,
    MAX(user_agent) AS user_agent,
    MAX(time) AS time,
    IF (@domain = "-" AND @repo = "-" AND @owner = "-", REGEXP_EXTRACT(MAX(url), "https://([^/]+)/", 1), MAX(url)) AS url,
    MAX(LCP) AS LCP,
    MAX(FID) AS FID,
    MAX(CLS) AS CLS,
    MAX(referer) AS referer,
    MAX(weight) AS weight,
    id
  FROM current_data 
WHERE url LIKE CONCAT("https://", @domain, "%") OR url LIKE CONCAT("https://%", @repo, "--", @owner, ".hlx%/") OR (@domain = "-" AND @repo = "-" AND @owner = "-")
GROUP BY id),
previous_rum_by_id AS (
    SELECT 
    IF(MAX(LCP) IS NULL, NULL, IF(MAX(LCP) < 2500, TRUE, FALSE)) AS lcpgood,
    IF(MAX(FID) IS NULL, NULL, IF(MAX(FID) < 100, TRUE, FALSE)) AS fidgood,
    IF(MAX(CLS) IS NULL, NULL, IF(MAX(CLS) < 0.1, TRUE, FALSE)) AS clsgood,
    IF(MAX(LCP) IS NULL, NULL, IF(MAX(LCP) > 4000, TRUE, FALSE)) AS lcpbad,
    IF(MAX(FID) IS NULL, NULL, IF(MAX(FID) > 300, TRUE, FALSE)) AS fidbad,
    IF(MAX(CLS) IS NULL, NULL, IF(MAX(CLS) > 0.25, TRUE, FALSE)) AS clsbad,
    MAX(host) AS host,
    MAX(user_agent) AS user_agent,
    MAX(time) AS time,
    IF (@domain = "-" AND @repo = "-" AND @owner = "-", REGEXP_EXTRACT(MAX(url), "https://([^/]+)/", 1), MAX(url)) AS url,
    MAX(LCP) AS LCP,
    MAX(FID) AS FID,
    MAX(CLS) AS CLS,
    MAX(referer) AS referer,
    MAX(weight) AS weight,
    id
  FROM previous_data 
WHERE url LIKE CONCAT("https://", @domain, "%") OR url LIKE CONCAT("https://%", @repo, "--", @owner, ".hlx%/") OR (@domain = "-" AND @repo = "-" AND @owner = "-")
GROUP BY id),
current_rum_by_url_and_weight AS (
    SELECT 
        CAST(100 * IF(COUNTIF(lcpgood IS NOT NULL) != 0, COUNTIF(lcpgood = TRUE)/COUNTIF(lcpgood IS NOT NULL), NULL) AS INT64) AS lcpgood,
        CAST(100 * IF(COUNTIF(fidgood IS NOT NULL) != 0, COUNTIF(fidgood = TRUE)/COUNTIF(fidgood IS NOT NULL), NULL) AS INT64) AS fidgood,
        CAST(100 * IF(COUNTIF(clsgood IS NOT NULL) != 0, COUNTIF(clsgood = TRUE)/COUNTIF(clsgood IS NOT NULL), NULL) AS INT64) AS clsgood,
        CAST(100 * IF(COUNTIF(lcpbad IS NOT NULL) != 0, COUNTIF(lcpbad = TRUE)/COUNTIF(lcpbad IS NOT NULL), NULL) AS INT64) AS lcpbad,
        CAST(100 * IF(COUNTIF(fidbad IS NOT NULL) != 0, COUNTIF(fidbad = TRUE)/COUNTIF(fidbad IS NOT NULL), NULL) AS INT64) AS fidbad,
        CAST(100 * IF(COUNTIF(clsbad IS NOT NULL) != 0, COUNTIF(clsbad = TRUE)/COUNTIF(clsbad IS NOT NULL), NULL) AS INT64) AS clsbad,
        CAST(APPROX_QUANTILES(LCP, 100)[OFFSET(75)] AS INT64) AS avglcp,
        CAST(APPROX_QUANTILES(FID, 100)[OFFSET(75)] AS INT64) AS avgfid,
        ROUND(APPROX_QUANTILES(CLS, 100)[OFFSET(75)], 3) AS avgcls,
        COUNT(id) AS events,
        weight, url
    FROM current_rum_by_id
    GROUP BY url, weight
    ORDER BY events DESC
), 
previous_rum_by_url_and_weight AS (
    SELECT 
        CAST(100 * IF(COUNTIF(lcpgood IS NOT NULL) != 0, COUNTIF(lcpgood = TRUE)/COUNTIF(lcpgood IS NOT NULL), NULL) AS INT64) AS lcpgood,
        CAST(100 * IF(COUNTIF(fidgood IS NOT NULL) != 0, COUNTIF(fidgood = TRUE)/COUNTIF(fidgood IS NOT NULL), NULL) AS INT64) AS fidgood,
        CAST(100 * IF(COUNTIF(clsgood IS NOT NULL) != 0, COUNTIF(clsgood = TRUE)/COUNTIF(clsgood IS NOT NULL), NULL) AS INT64) AS clsgood,
        CAST(100 * IF(COUNTIF(lcpbad IS NOT NULL) != 0, COUNTIF(lcpbad = TRUE)/COUNTIF(lcpbad IS NOT NULL), NULL) AS INT64) AS lcpbad,
        CAST(100 * IF(COUNTIF(fidbad IS NOT NULL) != 0, COUNTIF(fidbad = TRUE)/COUNTIF(fidbad IS NOT NULL), NULL) AS INT64) AS fidbad,
        CAST(100 * IF(COUNTIF(clsbad IS NOT NULL) != 0, COUNTIF(clsbad = TRUE)/COUNTIF(clsbad IS NOT NULL), NULL) AS INT64) AS clsbad,
        CAST(APPROX_QUANTILES(LCP, 100)[OFFSET(75)] AS INT64) AS avglcp,
        CAST(APPROX_QUANTILES(FID, 100)[OFFSET(75)] AS INT64) AS avgfid,
        ROUND(APPROX_QUANTILES(CLS, 100)[OFFSET(75)], 3) AS avgcls,
        COUNT(id) AS events,
        weight, url
    FROM previous_rum_by_id
    GROUP BY url, weight
    ORDER BY events DESC
), 
current_rum_by_url AS (
  SELECT 
      SUM(lcpgood * weight) / SUM(weight) AS lcpgood, 
      SUM(fidgood * weight) / SUM(weight) AS fidgood, 
      SUM(clsgood * weight) / SUM(weight) AS clsgood,
      SUM(lcpbad * weight) / SUM(weight) AS lcpbad, 
      SUM(fidbad * weight) / SUM(weight) AS fidbad, 
      SUM(clsbad * weight) / SUM(weight) AS clsbad,
      SUM(avglcp * weight) / SUM(weight) AS avglcp, 
      SUM(avgfid * weight) / SUM(weight) AS avgfid, 
      ROUND(SUM(avgcls * weight) / SUM(weight), 3) AS avgcls,
      SUM(events * weight) AS pageviews,
      url

  FROM current_rum_by_url_and_weight
  GROUP BY url
  ORDER BY pageviews DESC
), 
previous_rum_by_url AS (
  SELECT 
      SUM(lcpgood * weight) / SUM(weight) AS lcpgood, 
      SUM(fidgood * weight) / SUM(weight) AS fidgood, 
      SUM(clsgood * weight) / SUM(weight) AS clsgood,
      SUM(lcpbad * weight) / SUM(weight) AS lcpbad, 
      SUM(fidbad * weight) / SUM(weight) AS fidbad, 
      SUM(clsbad * weight) / SUM(weight) AS clsbad,
      SUM(avglcp * weight) / SUM(weight) AS avglcp, 
      SUM(avgfid * weight) / SUM(weight) AS avgfid, 
      ROUND(SUM(avgcls * weight) / SUM(weight), 3) AS avgcls,
      SUM(events * weight) AS pageviews,
      url

  FROM previous_rum_by_url_and_weight
  GROUP BY url
  ORDER BY pageviews DESC
),
current_event_count AS (
  SELECT SUM(events) AS allevents FROM (
    SELECT MAX(weight) AS events, id 
    FROM current_data
    GROUP BY id
  )
),
previous_event_count AS (
  SELECT SUM(events) AS allevents FROM (
    SELECT MAX(weight) AS events, id 
    FROM previous_data
    GROUP BY id
  )
),
current_truncated_rum_by_url AS (
  SELECT 
      CAST(SUM(lcpgood * pageviews) / SUM(pageviews) AS INT64) AS lcpgood, 
      CAST(SUM(fidgood * pageviews) / SUM(pageviews) AS INT64) AS fidgood, 
      CAST(SUM(clsgood * pageviews) / SUM(pageviews) AS INT64) AS clsgood,
      CAST(SUM(lcpbad * pageviews) / SUM(pageviews) AS INT64) AS lcpbad, 
      CAST(SUM(fidbad * pageviews) / SUM(pageviews) AS INT64) AS fidbad, 
      CAST(SUM(clsbad * pageviews) / SUM(pageviews) AS INT64) AS clsbad,
      CAST(SUM(avglcp * pageviews) / SUM(pageviews) AS INT64) AS avglcp, 
      CAST(SUM(avgfid * pageviews) / SUM(pageviews) AS INT64) AS avgfid, 
      ROUND(SUM(avgcls * pageviews) / SUM(pageviews), 3) AS avgcls,
      SUM(pageviews) AS pageviews,
      100 * SUM(pageviews) / MAX(allevents) AS rumshare,
      IF(rank > @limit, "Other", url) AS url 
  FROM (SELECT ROW_NUMBER() OVER (ORDER BY pageviews DESC) AS rank, * FROM current_rum_by_url), current_event_count 
  GROUP BY url
),
previous_truncated_rum_by_url AS (
  SELECT 
      CAST(SUM(lcpgood * pageviews) / SUM(pageviews) AS INT64) AS lcpgood, 
      CAST(SUM(fidgood * pageviews) / SUM(pageviews) AS INT64) AS fidgood, 
      CAST(SUM(clsgood * pageviews) / SUM(pageviews) AS INT64) AS clsgood,
      CAST(SUM(lcpbad * pageviews) / SUM(pageviews) AS INT64) AS lcpbad, 
      CAST(SUM(fidbad * pageviews) / SUM(pageviews) AS INT64) AS fidbad, 
      CAST(SUM(clsbad * pageviews) / SUM(pageviews) AS INT64) AS clsbad,
      CAST(SUM(avglcp * pageviews) / SUM(pageviews) AS INT64) AS avglcp, 
      CAST(SUM(avgfid * pageviews) / SUM(pageviews) AS INT64) AS avgfid, 
      ROUND(SUM(avgcls * pageviews) / SUM(pageviews), 3) AS avgcls,
      SUM(pageviews) AS pageviews,
      100 * SUM(pageviews) / MAX(allevents) AS rumshare,
      IF(rank > @limit, "Other", url) AS url 
  FROM (SELECT ROW_NUMBER() OVER (ORDER BY pageviews DESC) AS rank, * FROM previous_rum_by_url), previous_event_count 
  GROUP BY url
)
SELECT * FROM current_truncated_rum_by_url JOIN previous_truncated_rum_by_url USING (url)
