CREATE OR REPLACE TABLE
FUNCTION `helix-225321.helix_rum.EVENTS_V5`( -- noqa: PRS
    filterurl STRING,
    days_offset INT64,
    days_count INT64,
    day_min STRING,
    day_max STRING,
    timezone STRING,
    deviceclass STRING,
    domainkey STRING) AS (
  WITH
    rawrum AS (
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
        TTFB,
        referer,
        id,
        source,
        target,
        weight,
        checkpoint
      FROM
        `helix-225321.helix_rum.cluster_cloudflare`
      UNION ALL
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
        TTFB,
        referer,
        id,
        source,
        target,
        weight,
        checkpoint
      FROM
        `helix-225321.helix_rum.cluster`
    ),
    validkeys AS (
    SELECT
      *
    FROM
      `helix-225321.helix_reporting.domain_keys`
    WHERE
      key_bytes = SHA512(domainkey)
      AND (revoke_date IS NULL
        OR revoke_date > CURRENT_DATE(timezone))
      AND (
        hostname_prefix = ""
        OR filterurl LIKE CONCAT("%.", hostname_prefix)
        OR filterurl LIKE CONCAT("%.", hostname_prefix, "/%")
        OR filterurl LIKE CONCAT(hostname_prefix)
        OR filterurl LIKE CONCAT(hostname_prefix, "/%")
        -- handle comma-separated list of urls, remove spaces and trailing comma
        OR hostname_prefix IN (SELECT * FROM helix_rum.URLS_FROM_LIST(filterurl))
      )
    )
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
      TTFB,
      referer,
      id,
      source,
      target,
      weight,
      checkpoint
   FROM rawrum AS rumdata
  JOIN
    validkeys
  ON
    ( rumdata.url LIKE CONCAT("https://%.", validkeys.hostname_prefix, "/%")
      OR rumdata.url LIKE CONCAT("https://", validkeys.hostname_prefix, "/%")
      OR validkeys.hostname_prefix = "" )
  WHERE
     -- ignore invalid weights
    weight IN (1,10,20,100,1000)
    AND
    (
      (
        helix_rum.MATCH_URLS_V5(url, filterurl)
        AND
        IF(
          filterurl = '-', TRUE,
          (hostname = SPLIT(filterurl, '/')[OFFSET(0)])
          OR (
            filterurl LIKE 'localhost:%'
            AND hostname = 'localhost'
          )
        )
      )
      -- handle comma-separated list of urls, remove spaces and trailing comma
      OR hostname IN (SELECT * FROM helix_rum.URLS_FROM_LIST(filterurl))
    )
    AND
  IF
    (days_offset >= 0, DATETIME_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, helix_rum.CLEAN_TIMEZONE(timezone)), INTERVAL days_offset DAY), TIMESTAMP_ADD(TIMESTAMP(day_max, helix_rum.CLEAN_TIMEZONE(timezone)), INTERVAL 1 DAY)) > time
    AND
  IF
    (days_count >= 0, DATETIME_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, helix_rum.CLEAN_TIMEZONE(timezone)), INTERVAL (days_offset + days_count) DAY), TIMESTAMP(day_min, helix_rum.CLEAN_TIMEZONE(timezone))) <= time
    AND helix_rum.CLUSTER_FILTERCLASS(user_agent,
      deviceclass)
    -- ignore invalid hostnames
    AND REGEXP_CONTAINS(hostname, r'^[a-zA-Z0-9_\-./]*$') IS TRUE
    AND time < CURRENT_TIMESTAMP()
);
