--- description: Show lcps and lcps with conversion for specified domains from RUM data
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- interval: 30
--- offset: 0
--- startdate: 2023-05-01
--- enddate: 2023-06-01
--- timezone: UTC
--- url: -
--- device: all
--- domainkey: secret

-- currently hard-coded for www.bamboohr.com
WITH rum AS (
  SELECT
    id,
    DIV(CAST(lcp AS INT64), 100) * 100 AS rounded_lcp
  FROM `helix_rum.cluster`
  WHERE
    hostname = 'www.bamboohr.com'
    AND lcp IS NOT NULL
),

clicks AS (
  SELECT a.rounded_lcp
  FROM rum AS a
  INNER JOIN `helix_rum.cluster` AS b ON a.id = b.id
  WHERE
    b.hostname = 'www.bamboohr.com'
    AND b.checkpoint = 'click'
    AND b.target IN (
      'https://www.bamboohr.com/signup.php',
      'https://www.bamboohr.com/pl-pages/demo-request/',
      'https://www.bamboohr.com/signup/'
    )
),

buckets AS (
  SELECT * FROM UNNEST([
    0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
    1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000,
    2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900, 3000
  ]) AS bucket
)

SELECT
  a.bucket,
  COUNT(b.rounded_lcp) AS lcp_count,
  -- for some reason a subquery gave correct results while the left join commented below did not
  (
    SELECT COUNT(c.rounded_lcp) FROM clicks AS c WHERE c.rounded_lcp = a.bucket
  ) AS click_count
FROM buckets AS a
LEFT JOIN rum AS b ON a.bucket = b.rounded_lcp
--left join clicks c on a.bucket = c.rounded_lcp
GROUP BY a.bucket
ORDER BY a.bucket
