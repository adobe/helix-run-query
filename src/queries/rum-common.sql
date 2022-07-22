CREATE OR REPLACE FUNCTION helix_rum.CLUSTER_FILTERCLASS(user_agent STRING, device STRING) 
  RETURNS BOOLEAN
  AS (
    device = "all" OR 
    (device = "desktop" AND user_agent NOT LIKE "%Mobile%" AND user_agent LIKE "Mozilla%" ) OR 
    (device = "nobot" AND (
      user_agent NOT LIKE "%Amazon CloudFront%" OR
      user_agent NOT LIKE "%Apache Http Client%" OR
      user_agent NOT LIKE "%Asynchronous Http Client%" OR 
      user_agent NOT LIKE "%Axios"  OR 
      user_agent NOT LIKE "%Azureus%" OR 
      user_agent NOT LIKE "%Curl%" OR 
      user_agent NOT LIKE "%Guzzle%" OR 
      user_agent NOT LIKE "%Go-http-client%" OR 
      user_agent NOT LIKE "%Headless Chrome%" OR 
      user_agent NOT LIKE "%Java Client%" OR 
      user_agent NOT LIKE "%Jersey%" OR 
      user_agent NOT LIKE "%Node Oembed%" OR 
      user_agent NOT LIKE "%okhttp%" OR 
      user_agent NOT LIKE "%Python Requests%" OR 
      user_agent NOT LIKE "%Wget%" OR 
      user_agent NOT LIKE "%WinHTTP%" OR 
      user_agent NOT LIKE "%Fast HTTP%" OR 
      user_agent NOT LIKE "%GitHub Node Fetch"
    )) OR
    (device = "mobile" AND user_agent LIKE "%Mobile%") OR
    (device = "bot" AND user_agent NOT LIKE "Mozilla%"));

CREATE OR REPLACE TABLE FUNCTION helix_rum.CLUSTER_EVENTS(filterurl STRING, days_offset INT64, days_count INT64, day_min STRING, day_max STRING, timezone STRING, deviceclass STRING, filtergeneration STRING)
AS
  SELECT 
    *
  FROM `helix-225321.helix_rum.cluster` 
  WHERE IF(filterurl = '-', TRUE, (url LIKE CONCAT('https://', filterurl, '%')) OR (filterurl LIKE 'localhost%' AND url LIKE CONCAT('http://', filterurl, '%')))
  AND   IF(filterurl = '-', TRUE, (hostname = SPLIT(filterurl, '/')[OFFSET(0)]) OR (filterurl LIKE 'localhost:%' AND hostname = 'localhost'))
  AND   IF(days_offset >= 0, DATETIME_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, timezone), INTERVAL days_offset DAY),                TIMESTAMP(day_max, timezone)) >= time
  AND   IF(days_count >= 0,  DATETIME_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, timezone), INTERVAL (days_offset + days_count) DAY), TIMESTAMP(day_min, timezone)) <= time
  AND   helix_rum.CLUSTER_FILTERCLASS(user_agent, deviceclass)
  AND   IF(filtergeneration = '-', TRUE, generation = filtergeneration)
;

CREATE OR REPLACE TABLE FUNCTION helix_rum.CLUSTER_PAGEVIEWS(filterurl STRING, days_offset INT64, days_count INT64, day_min STRING, day_max STRING, timezone STRING, deviceclass STRING, filtergeneration STRING)
AS
  SELECT
    ANY_VALUE(hostname) AS hostname,
    ANY_VALUE(host) AS host,
    MAX(time) AS time,
    MAX(weight) AS pageviews,
    MAX(LCP) AS LCP,
    MAX(CLS) AS CLS,
    MAX(FID) AS FID,
    ANY_VALUE(generation) AS generation,
    ANY_VALUE(url) AS url,
    ANY_VALUE(referer) AS referer,
    ANY_VALUE(user_agent) AS user_agent,
    id
  FROM helix_rum.CLUSTER_EVENTS(filterurl, days_offset, days_count, day_min, day_max, timezone, deviceclass, filtergeneration)
  GROUP BY id;

CREATE OR REPLACE TABLE FUNCTION helix_rum.CLUSTER_CHECKPOINTS(filterurl STRING, days_offset INT64, days_count INT64, day_min STRING, day_max STRING, timezone STRING, deviceclass STRING, filtergeneration STRING)
AS
  SELECT
    ANY_VALUE(hostname) AS hostname,
    ANY_VALUE(host) AS host,
    MAX(time) AS time,
    checkpoint,
    source,
    target,
    MAX(weight) AS pageviews,
    ANY_VALUE(generation) AS generation,
    id,
    ANY_VALUE(url) AS url,
    ANY_VALUE(referer) AS referer,
    ANY_VALUE(user_agent) AS user_agent,
  FROM helix_rum.CLUSTER_EVENTS(filterurl, days_offset, days_count, day_min, day_max, timezone, deviceclass, filtergeneration)
  GROUP BY id, checkpoint, target, source;

# SELECT * FROM helix_rum.CLUSTER_PAGEVIEWS('blog.adobe.com', 1, 7, '', '', 'GMT', 'desktop', '-')
# ORDER BY time DESC
# LIMIT 10;

SELECT hostname, url, time FROM helix_rum.CLUSTER_CHECKPOINTS('localhost:3000/drafts', -1, -7, '2022-02-01', '2022-05-28', 'GMT', 'all', '-')
ORDER BY time DESC
LIMIT 10;