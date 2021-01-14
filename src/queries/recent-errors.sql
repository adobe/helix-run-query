--- description: log entries of requests with 50x status code.
--- Authorization: fastly
--- limit: 1000
--- fromMins: 60
--- toMins: 0
SELECT req_url AS url, COUNT(time_start_usec) AS count
FROM ( 
  ^myrequests
)
WHERE status_code >= "500" AND
time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)) AS STRING) AND
time_start_usec < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 MINUTE)) AS STRING)
GROUP BY url
ORDER BY count DESC
LIMIT 1000