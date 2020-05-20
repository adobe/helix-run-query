--- Authorization: none
--- Cache-Control: max-age=1296000
--- fromDays: 30
--- toDays: 0
--- limit: 100
with topurls as ( SELECT 
    req_http_X_Owner AS owner, 
    req_http_X_Repo AS repo,
    CONCAT(req_http_X_Owner, req_http_X_Repo) as name,
    req_url,
    COUNT(time_start_usec) AS requests,
  FROM `helix_logging_1McGRQOYFuABWBHyD8D4Ux.requests*`
  WHERE 
    resp_http_Content_Type LIKE "%html%" AND
    status_code = "200" AND
    req_url NOT LIKE "https://(null)(null)" AND
    time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @fromDays DAY)) AS STRING) AND
    time_start_usec < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @toDays DAY)) AS STRING)
  GROUP BY
    req_url,
    owner,
    repo
  ORDER BY
    requests DESC
) (
  with urls as 
    (
      SELECT a.req_url, a.name, a.requests
      FROM TOPURLS a
      INNER JOIN (
          SELECT distinct name, MAX(requests) reqs
          FROM TOPURLS
          GROUP BY name
        ) 
      b ON a.name = b.name AND a.requests = b.reqs order by a.requests desc
    )(
        select req_url 
        from urls d 
        where ( 
          select (
            select count(name) as count 
            from urls 
            where d.name = name group by name)) = 1 
        order by requests desc limit @limit)
      )