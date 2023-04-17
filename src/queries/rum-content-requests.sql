--- description: List number of content requests per day and hostname
--- startdate: 2022-01-01
--- enddate: 2022-01-02
--- timezone: UTC
--- limit: 1000
--- offset: 0
--- domainkey: secret
SELECT
  day,
  contentrequests,
  hostname
FROM (
  SELECT
    hostname,
    ROW_NUMBER() OVER (ORDER BY hostname) AS resrow,
    FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(time, DAY)) AS day,
    SUM(pageviews) AS contentrequests
  FROM
    helix_rum.PAGEVIEWS_V3(
      '-', # url
      CAST(@offset AS INT64), # offset
      -1, # days to fetch
      @startdate, # start date
      @enddate, # end date
      @timezone, # timezone
      'nobot', # deviceclass
      @domainkey # domainkey
    )
  WHERE (
    hostname NOT LIKE '%.hlx.live'
    AND hostname NOT LIKE '%.hlx3.live'
    AND hostname NOT LIKE '%.hlx.page'
    AND hostname NOT LIKE '%.hlx3.page'
    AND hostname NOT LIKE 'localhost'
    AND hostname NOT LIKE '127.0.0.1'
  )
  GROUP BY day, hostname
  ORDER BY hostname, day
)
WHERE resrow > CAST(@offset AS INT64) AND resrow <= CAST(@limit AS INT64)
