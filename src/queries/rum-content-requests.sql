--- description: List number of content requests per day and hostname
--- from: 2022-01-01
--- to: 2022-01-02
SELECT
  hostname,
  FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(time, DAY)) AS day,
  SUM(pageviews) AS contentrequests
FROM helix_rum.CLUSTER_PAGEVIEWS(
  '-', # url
  -1, # offset
  -1, # days to fetch
  @from, # start date
  @to, # end date
  'UTC', # timezone
  'all', # deviceclass
  '-' # not used, generation
)
WHERE (
  hostname NOT LIKE '%.hlx.live'
  AND hostname NOT LIKE '%.hlx3.live' AND
  hostname NOT LIKE '%.hlx.page' AND
  hostname NOT LIKE '%.hlx3.page' AND
  hostname NOT LIKE 'localhost' AND
  hostname NOT LIKE '127.0.0.1'
)
GROUP BY day, hostname
ORDER BY hostname, day
