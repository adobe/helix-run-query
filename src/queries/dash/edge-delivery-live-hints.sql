--- description: Get Edge Delivery domains with the first RUM collection in past 30 days yesterday
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- limit: 200
--- interval: 30
--- offset: 0
--- startdate: 2022-01-01
--- enddate: 2022-01-31
--- timezone: UTC
--- url: -
--- domainkey: secret

WITH hosts AS (
  SELECT
    hostname,
    DATE(MIN(TIMESTAMP_TRUNC(time, DAY, @timezone))) AS first_rum
  FROM
    helix_rum.EVENTS_V5(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      'all',
      @domainkey
    )
  WHERE
    checkpoint = 'top'
    AND NOT (
      host LIKE '%adobeaemcloud.net'
      AND hostname NOT LIKE '%adobeaemcloud.com'
      AND host != hostname
    )
  GROUP BY hostname
)

SELECT
  hostname,
  first_rum
FROM hosts
WHERE first_rum >= CURRENT_DATE(@timezone) - 1
LIMIT @limit
