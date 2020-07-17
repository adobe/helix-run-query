--- description: log entries of requests occuring in [fromMins, toMins].
--- Authorization: fastly
--- limit: 1000
--- contains: theblog--adobe.hlx.page
--- fromMins: 30
--- toMins: 0
SELECT *
FROM `helix_logging_1McGRQOYFuABWBHyD8D4Ux.requests*` 
WHERE REGEXP_CONTAINS(req_url, @contains) AND
time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromMins MINUTE)) AS STRING) AND
time_start_usec < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @toMins MINUTE)) AS STRING)
ORDER BY time_start_usec DESC
LIMIT @limit