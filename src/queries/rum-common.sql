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

CREATE OR REPLACE FUNCTION helix_rum.CLEAN_TIMEZONE(intimezone STRING)
  RETURNS STRING
  AS (
    CASE
      WHEN intimezone = "undefined" THEN "GMT"
      WHEN intimezone = "" THEN "GMT"
      ELSE intimezone 
    END
  );

CREATE OR REPLACE TABLE FUNCTION helix_rum.CLUSTER_EVENTS(filterurl STRING, days_offset INT64, days_count INT64, day_min STRING, day_max STRING, timezone STRING, deviceclass STRING, filtergeneration STRING)
AS
  SELECT 
    *
  FROM `helix-225321.helix_rum.cluster` 
  WHERE IF(filterurl = '-', TRUE, (url LIKE CONCAT('https://', filterurl, '%')) OR (filterurl LIKE 'localhost%' AND url LIKE CONCAT('http://', filterurl, '%')))
  AND   IF(filterurl = '-', TRUE, (hostname = SPLIT(filterurl, '/')[OFFSET(0)]) OR (filterurl LIKE 'localhost:%' AND hostname = 'localhost'))
  AND   IF(days_offset >= 0, DATETIME_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, helix_rum.CLEAN_TIMEZONE(timezone)), INTERVAL days_offset DAY),                TIMESTAMP(day_max, helix_rum.CLEAN_TIMEZONE(timezone))) >= time
  AND   IF(days_count >= 0,  DATETIME_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, helix_rum.CLEAN_TIMEZONE(timezone)), INTERVAL (days_offset + days_count) DAY), TIMESTAMP(day_min, helix_rum.CLEAN_TIMEZONE(timezone))) <= time
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

CREATE OR REPLACE TABLE
  FUNCTION `helix-225321.helix_rum.EVENTS_V3`(filterurl STRING,
    days_offset INT64,
    days_count INT64,
    day_min STRING,
    day_max STRING,
    timezone STRING,
    deviceclass STRING,
    domainkey STRING) AS (
  WITH
    validkeys AS (
    SELECT
      *
    FROM
      `helix-225321.helix_reporting.domain_keys`
    WHERE
      key_bytes = SHA512(domainkey)
      AND (revoke_date IS NULL
        OR revoke_date > CURRENT_DATE(timezone))
      AND (hostname_prefix = ""
        OR filterurl LIKE CONCAT("%.", hostname_prefix)
        OR filterurl LIKE CONCAT("%.", hostname_prefix, "/%")
        OR filterurl LIKE CONCAT(hostname_prefix)
        OR filterurl LIKE CONCAT(hostname_prefix, "/%")))
  SELECT
    hostname,
    host,
    user_agent,
    time,
    url,
    LCP,
    FID,
    INP,
    CLS,
    referer,
    id,
    SOURCE,
    TARGET,
    weight,
    checkpoint
  FROM
    `helix-225321.helix_rum.cluster` AS rumdata
  JOIN
    validkeys
  ON
    ( rumdata.url LIKE CONCAT("https://%.", validkeys.hostname_prefix, "/%")
      OR rumdata.url LIKE CONCAT("https://", validkeys.hostname_prefix, "/%")
      OR validkeys.hostname_prefix = "" )
  WHERE
    ( (filterurl = '-') # any URL goes
      OR (url LIKE CONCAT('https://', filterurl, '%')) # default behavior,
      OR (filterurl LIKE 'localhost%'
        AND url LIKE CONCAT('http://', filterurl, '%')) # localhost
      OR (ENDS_WITH(filterurl, '$')
        AND url = CONCAT('https://', REPLACE(filterurl, '$', ''))) # strict URL
      OR (ENDS_WITH(filterurl, '?')
        AND url = CONCAT('https://', REPLACE(filterurl, '?', ''))) # strict URL, but URL params are supported
      )
    AND
  IF
    (filterurl = '-', TRUE, (hostname = SPLIT(filterurl, '/')[
      OFFSET
        (0)])
      OR (filterurl LIKE 'localhost:%'
        AND hostname = 'localhost'))
    AND
  IF
    (days_offset >= 0, DATETIME_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, helix_rum.CLEAN_TIMEZONE(timezone)), INTERVAL days_offset DAY), TIMESTAMP(day_max, helix_rum.CLEAN_TIMEZONE(timezone))) >= time
    AND
  IF
    (days_count >= 0, DATETIME_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, helix_rum.CLEAN_TIMEZONE(timezone)), INTERVAL (days_offset + days_count) DAY), TIMESTAMP(day_min, helix_rum.CLEAN_TIMEZONE(timezone))) <= time
    AND helix_rum.CLUSTER_FILTERCLASS(user_agent,
      deviceclass) );
CREATE OR REPLACE TABLE
  FUNCTION helix_rum.PAGEVIEWS_V3(filterurl STRING,
    days_offset INT64,
    days_count INT64,
    day_min STRING,
    day_max STRING,
    timezone STRING,
    deviceclass STRING,
    domainkey STRING) AS
SELECT
  ANY_VALUE(hostname) AS hostname,
  ANY_VALUE(host) AS host,
  MAX(time) AS time,
  MAX(weight) AS pageviews,
  MAX(LCP) AS LCP,
  MAX(CLS) AS CLS,
  MAX(FID) AS FID,
  MAX(INP) AS INP,
  ANY_VALUE(url) AS url,
  ANY_VALUE(referer) AS referer,
  ANY_VALUE(user_agent) AS user_agent,
  id
FROM
  helix_rum.EVENTS_V3(filterurl,
    days_offset,
    days_count,
    day_min,
    day_max,
    timezone,
    deviceclass,
    domainkey)
GROUP BY
  id;
CREATE OR REPLACE TABLE
  FUNCTION helix_rum.CHECKPOINTS_V3(filterurl STRING,
    days_offset INT64,
    days_count INT64,
    day_min STRING,
    day_max STRING,
    timezone STRING,
    deviceclass STRING,
    domainkey STRING) AS
SELECT
  ANY_VALUE(hostname) AS hostname,
  ANY_VALUE(host) AS host,
  MAX(time) AS time,
  checkpoint,
  source,
  target,
  MAX(weight) AS pageviews,
  id,
  ANY_VALUE(url) AS url,
  ANY_VALUE(referer) AS referer,
  ANY_VALUE(user_agent) AS user_agent,
FROM
  helix_rum.EVENTS_V3(filterurl,
    days_offset,
    days_count,
    day_min,
    day_max,
    timezone,
    deviceclass,
    domainkey)
GROUP BY
  id,
  checkpoint,
  target,
  source;

CREATE OR REPLACE PROCEDURE
  helix_reporting.ROTATE_DOMAIN_KEYS( IN indomainkey STRING,
    IN inurl STRING,
    IN intimezone STRING,
    IN ingraceperiod INT64,
    IN inexpirydate STRING,
    IN innewkey STRING,
    IN inreadonly BOOL,
    IN innote STRING)
BEGIN
-- allow multiple domains to be passed in as comma-separated value
DECLARE urls ARRAY<STRING>;
SET urls =  SPLIT(inurl, ',');

UPDATE `helix-225321.helix_reporting.domain_keys`
SET revoke_date = DATE_ADD(CURRENT_DATE(intimezone), INTERVAL ingraceperiod DAY)
WHERE
  # hostname prefix matches
  hostname_prefix IN (SELECT * from UNNEST(urls))
  # key is still valid
  AND (revoke_date IS NULL
    OR revoke_date > CURRENT_DATE(intimezone))
  AND ingraceperiod > 0;

INSERT INTO `helix-225321.helix_reporting.domain_keys` (
  hostname_prefix,
  key_bytes,
  revoke_date,
  readonly,
  create_date,
  parent_key_bytes,
  note
)
SELECT
  *,
  SHA512(innewkey),
  IF(inexpirydate = "-", NULL, DATE(inexpirydate)),
  inreadonly,
  CURRENT_DATE(intimezone),
  SHA512(indomainkey),
  innote
FROM UNNEST(urls);

END

CREATE OR REPLACE TABLE FUNCTION helix_reporting.DOMAINKEY_PRIVS_ALL(domainkey STRING, timezone STRING)
AS (
  WITH key AS (
    SELECT hostname_prefix, readonly
    FROM `helix-225321.helix_reporting.domain_keys`
    WHERE
      key_bytes = SHA512(domainkey)
      AND (
        revoke_date IS NULL
        OR revoke_date > CURRENT_DATE(timezone)
      )
  )
  SELECT COALESCE(
    (
      SELECT IF(hostname_prefix = '', true, false)
      FROM key
    ),
    false
  ) AS read,
  COALESCE(
    (
      SELECT IF(hostname_prefix = '' AND readonly = false, true, false)
      FROM key
    ),
    false
  ) AS write
)


# SELECT * FROM helix_rum.CLUSTER_PAGEVIEWS('blog.adobe.com', 1, 7, '', '', 'GMT', 'desktop', '-')
# ORDER BY time DESC
# LIMIT 10;

SELECT hostname, url, time FROM helix_rum.CLUSTER_CHECKPOINTS('localhost:3000/drafts', -1, -7, '2022-02-01', '2022-05-28', 'GMT', 'all', '-')
ORDER BY time DESC
LIMIT 10;