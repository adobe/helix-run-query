--- Authorization: none
--- Cache-Control: max-age=1296000
--- contains: https://theblog--davidnuescheler.hlx.page/ms/archive/posts
--- repo: theblog
--- owner: davidnuescheler
--- fromDays: 30
--- toDays: 0
--- limit: 10
SELECT req_url, count(req_http_X_CDN_Request_ID) as reqs 
  FROM `helix_logging_1McGRQOYFuABWBHyD8D4Ux.requests*`
  WHERE 
    REGEXP_CONTAINS(req_url, @contains) AND
    req_http_X_Repo = @repo AND
    req_http_X_Owner = @owner AND
    resp_http_Content_Type = "text/html; charset=UTF-8" AND
    status_code = "200" AND
    time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromDays DAY)) AS STRING) AND
    time_start_usec < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @toDays DAY)) AS STRING)
  GROUP BY req_url
  ORDER BY reqs DESC
LIMIT @limit
