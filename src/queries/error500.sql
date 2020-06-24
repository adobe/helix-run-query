--- description: log entries of requests with 50x status code.
--- Authorization: none
--- limit: 1000
--- fromMins: 30
--- toMins: 0
SELECT *
FROM `helix_logging_1McGRQOYFuABWBHyD8D4Ux.requests*` 
WHERE status_code >= "500" AND
time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromMins MINUTE)) AS STRING) AND
time_start_usec < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @toMins MINUTE)) AS STRING)
ORDER BY time_start_usec DESC
LIMIT @limit