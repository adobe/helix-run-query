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
      KEY = domainkey
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
    source,
    target,
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
        CASE
          WHEN filterurl = '-'             THEN TRUE # all URLs
          WHEN filterurl LIKE 'localhost%' THEN url LIKE CONCAT('http://', filterurl, '%')
          WHEN ENDS_WITH(filterurl, '$')   THEN url = CONCAT('https://', REPLACE(filterurl, '$', ''))
          WHEN ENDS_WITH(filterurl, '?')   THEN url LIKE CONCAT('https://', filterurl, '%') OR url = CONCAT('https://', REPLACE(filterurl, '$', ''))
          ELSE url LIKE CONCAT('https://', filterurl, '%')
        END
      AND
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
