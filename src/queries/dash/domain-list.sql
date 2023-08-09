--- description: List of domains along with some summary data.
--- Access-Control-Allow-Origin: *
--- Cache-Control: max-age=3600
--- timezone: UTC
--- device: all
--- domainkey: secret

WITH pvs AS (
  SELECT
    weight,
    REGEXP_REPLACE(hostname, r'www.', '') AS hostname,
    COUNT(DISTINCT id) AS ids,
    FORMAT_DATE('%Y-%b', time) AS month,
    MIN(time) AS first_visit,
    MAX(time) AS last_visit
  FROM
    helix_rum.EVENTS_V3(
      '-',
      -1,
      -1,
      '2020-01-01',
      '2099-12-31',
      @timezone,
      @device,
      @domainkey
    )
  WHERE
    hostname != ''
    AND hostname != '127.0.0.1'
    AND hostname NOT LIKE 'localhost%'
    AND hostname NOT LIKE '%.hlx.page'
    AND hostname NOT LIKE '%.hlx3.page'
    AND hostname NOT LIKE '%.hlx.live'
    AND hostname NOT LIKE '%.helix3.dev'
    AND hostname NOT LIKE '%.sharepoint.com'
    AND hostname NOT LIKE '%.google.com'
    OR hostname = 'www.hlx.live'
  GROUP BY month, weight, hostname
),

month_pvs AS (
  SELECT
    hostname,
    month,
    SUM(weight * ids) AS estimated_pvs,
    FORMAT_DATE('%F', MIN(first_visit)) AS first_visit,
    FORMAT_DATE('%F', MAX(last_visit)) AS last_visit
  FROM pvs
  GROUP BY hostname, month
),

total_pvs AS (
  SELECT
    hostname,
    FORMAT_DATE('%F', MIN(first_visit)) AS first_visit,
    FORMAT_DATE('%F', MAX(last_visit)) AS last_visit,
    SUM(weight * ids) AS estimated_pvs
  FROM pvs
  GROUP BY hostname
),

domains AS (
  SELECT
    a.hostname,
    a.first_visit,
    a.last_visit,
    b.estimated_pvs AS current_month_visits,
    a.estimated_pvs AS total_visits
  FROM total_pvs AS a
  LEFT JOIN
    month_pvs AS b
    ON
      a.hostname = b.hostname AND b.month = FORMAT_DATE('%Y-%b', CURRENT_DATE())
  GROUP BY
    a.hostname, a.first_visit, a.last_visit, a.estimated_pvs, b.estimated_pvs
)

SELECT
  hostname,
  first_visit,
  last_visit,
  current_month_visits,
  total_visits
FROM domains
WHERE
  total_visits >= 1000
  AND DATE(last_visit) > (CURRENT_DATE() - 90)
ORDER BY total_visits DESC, current_month_visits DESC, first_visit DESC
