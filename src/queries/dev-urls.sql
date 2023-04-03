--- description: Get list of URLs visited during development on a branch
--- Authorization: none
--- Access-Control-Allow-Origin: *
--- owner: adobe
--- repo: helix-website
--- branch: main
SELECT
  req_http_x_owner,
  req_http_x_repo,
  req_http_x_ref,
  req_url,
  COUNT(time_start_usec) AS requests
FROM `helix-225321.helix_logging_7TvULgs0Xnls4q3R8tawdg.requests*`
WHERE req_http_x_owner = @owner
  AND req_http_x_repo = @repo
  AND req_http_x_ref = @branch
  AND resp_http_content_type = "text/html; charset=utf-8"
  AND status_code = "200"
  # AND req_http_Referer LIKE "http://localhost:%"
  AND req_http_x_url NOT LIKE "%.plain.html%"
  AND req_url NOT LIKE "%/head.html%"
  AND _table_suffix = CONCAT(
    CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String),
    LPAD(CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, "0")
  )
GROUP BY req_http_x_owner, req_http_x_repo, req_http_x_ref, req_url
ORDER BY requests DESC
LIMIT 5
