--- description: Show core web vitals for specified domains for specified dates from RUM data
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

WITH rum AS (
  SELECT
    lcp,
    fid,
    cls,
    FORMAT_DATE('%F', time) AS date
  FROM
    helix_rum.EVENTS_V3(
      @url,
      CAST(@offset AS INT64),
      CAST(@interval AS INT64),
      @startdate,
      @enddate,
      @timezone,
      @device,
      @domainkey
    )
  WHERE
    hostname = @url
    AND (
      lcp IS NOT NULL
      OR fid IS NOT NULL
      OR cls IS NOT NULL
    )
),

lcps AS (
  SELECT DISTINCT
    date,
    CAST(PERCENTILE_CONT(lcp, 0.5) OVER (PARTITION BY date) AS INT64) AS lcp50,
    CAST(PERCENTILE_CONT(lcp, 0.75) OVER (PARTITION BY date) AS INT64) AS lcp75,
    CAST(PERCENTILE_CONT(lcp, 0.9) OVER (PARTITION BY date) AS INT64) AS lcp90
  FROM rum
  GROUP BY date, lcp
),

fids AS (
  SELECT DISTINCT
    date,
    CAST(PERCENTILE_CONT(fid, 0.5) OVER (PARTITION BY date) AS INT64) AS fid50,
    CAST(PERCENTILE_CONT(fid, 0.75) OVER (PARTITION BY date) AS INT64) AS fid75,
    CAST(PERCENTILE_CONT(fid, 0.9) OVER (PARTITION BY date) AS INT64) AS fid90
  FROM rum
  GROUP BY date, fid
),

clss AS (
  SELECT DISTINCT
    date,
    ROUND(PERCENTILE_CONT(cls, 0.5) OVER (PARTITION BY date), 3) AS cls50,
    ROUND(PERCENTILE_CONT(cls, 0.75) OVER (PARTITION BY date), 3) AS cls75,
    ROUND(PERCENTILE_CONT(cls, 0.90) OVER (PARTITION BY date), 3) AS cls90
  FROM rum
  GROUP BY date, cls
)

SELECT
  lcps.date,
  lcps.lcp50,
  lcps.lcp75,
  lcps.lcp90,
  fids.fid50,
  fids.fid75,
  fids.fid90,
  clss.cls50,
  clss.cls75,
  clss.cls90
FROM lcps
LEFT JOIN fids ON lcps.date = fids.date
LEFT JOIN clss ON lcps.date = clss.date
ORDER BY lcps.date
