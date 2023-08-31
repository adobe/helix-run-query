--- description: Get monthly page views for a site according to Helix RUM data
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 365
--- offset: 0
--- url: -
--- timezone: UTC
--- device: all
--- domainkey: secret

-- prepare two strings to insert into query below
-- pivot command requires specific values, not dynamic results
DECLARE months12 STRING;
DECLARE months3 STRING;

SET months12 = (
  SELECT
    CONCAT(
      '("',
      STRING_AGG(DISTINCT FORMAT_DATE('%Y-%b', alldays), '", "'),
      '")'
    )
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY(CURRENT_DATE('UTC') - 365, CURRENT_DATE('UTC') - 1)
    ) AS alldays
  WHERE FORMAT_DATE('%d', alldays) = '01'
);
SET months3 = (
  SELECT
    CONCAT(
      '("',
      STRING_AGG(DISTINCT FORMAT_DATE('%Y-%b', alldays), '", "'),
      '")'
    )
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY(CURRENT_DATE('UTC') - 90, CURRENT_DATE('UTC') - 1)
    ) AS alldays
);

-- workaround to put dynamic list of specific values into PIVOT command
-- otherwise month/year would need to be hardcoded
EXECUTE IMMEDIATE format( -- noqa: PRS
"""
WITH pvs AS (
  SELECT
    SUM(pageviews) AS pageviews,
    REGEXP_REPLACE(hostname, r'^www.', '') AS hostname,
    FORMAT_DATE('%%Y-%%b', time) AS month
  FROM
    helix_rum.PAGEVIEWS_V3(
      '%s',
      %d,
      %d,
      '',
      '',
      '%s',
      '%s',
      '%s'
    )
  WHERE
    hostname != ''
    AND NOT REGEXP_CONTAINS(hostname, r'^\\d+\\.\\d+\\.\\d+\\.\\d+$') -- IP addresses
    AND hostname NOT LIKE 'localhost%%'
    AND hostname NOT LIKE '%%.hlx.page'
    AND hostname NOT LIKE '%%.hlx3.page'
    AND hostname NOT LIKE '%%.hlx.live'
    AND hostname NOT LIKE '%%.helix3.dev'
    AND hostname NOT LIKE '%%.sharepoint.com'
    AND hostname NOT LIKE '%%.google.com'
    AND hostname NOT LIKE '%%.edison.pfizer' -- not live
    AND hostname NOT LIKE '%%.web.pfizer'
    OR hostname = 'www.hlx.live'
  GROUP BY month, hostname
),

total_pvs AS (
  SELECT
    hostname,
    month,
    SUM(pageviews) AS estimated_pvs
  FROM pvs
  GROUP BY hostname, month
),

grid AS (
  SELECT * FROM
  (
    SELECT hostname, month, estimated_pvs
    FROM total_pvs
  )
  PIVOT
  (
    ANY_VALUE(estimated_pvs)
    FOR month IN %s
  )
)

SELECT *
FROM grid
WHERE hostname IN (SELECT DISTINCT hostname FROM total_pvs WHERE month IN %s AND estimated_pvs >= 1000)
ORDER BY hostname
""", @url, @offset, @interval, @timezone, @device, @domainkey, months12, months3);
