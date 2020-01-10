--- Authorization: fastly
--- Cache-Control: max-age=1296000
--- fromDays: 30
--- toDays: 0
--- limit: 10
SELECT REGEXP_EXTRACT(req_url, "/posts/[^.]*") as blog , count(req_http_X_CDN_Request_ID) as reqs 
  FROM `helix_logging_1McGRQOYFuABWBHyD8D4Ux.requests*`
  WHERE 
    req_url LIKE "%/posts/%" 
    AND
    status_code = "200" AND
    time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromDays DAY)) AS STRING) AND
    time_start_usec < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @toDays DAY)) AS STRING)
  GROUP BY
    blog
  ORDER BY reqs DESC
LIMIT @limit