/*
 * Copyright 2020 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
*/
--- description: most visited urls within hlx3.page
--- Authorization: none
--- Cache-Control: max-age=1296000
--- fromDays: 30
--- toDays: 0
--- threshold: 100
--- limit: 100
WITH num_visits AS (
  SELECT 
    COUNT(req_url) AS num_reqs,
    CONCAT(req_http_X_Owner, '--', req_http_X_Repo) AS repo_name,
    req_url,
  FROM `helix_logging_7TvULgs0Xnls4q3R8tawdg.requests*`
  WHERE 
    # use date partitioning to reduce query size
    _TABLE_SUFFIX <= CONCAT(CAST(EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) AS String), LPAD(CAST(EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AS String), 2, "0")) AND
    _TABLE_SUFFIX >= CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS String), LPAD(CAST(EXTRACT(MONTH FROM TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS String), 2, "0")) AND
    resp_http_Content_Type LIKE "text/html%" AND
    status_code = "200" AND
    time_start_usec > CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS STRING) AND
    time_start_usec < CAST(UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 DAY)) AS STRING)
  GROUP BY req_url, repo_name
)
(
  WITH max_visits AS (
    SELECT 
      MAX(num_reqs) AS max_reqs,
      repo_name,
    FROM num_visits
    GROUP BY repo_name
  )
  SELECT m.max_reqs, m.repo_name, n.req_url
  FROM 
    max_visits AS m, 
    num_visits AS n
  WHERE 
    m.max_reqs > CAST(@threshold as INT64) AND
    m.max_reqs = n.num_reqs AND
    m.repo_name = n.repo_name AND 
    m.repo_name NOT IN (SELECT repo_name FROM `helix-225321.helix_logging_1McGRQOYFuABWBHyD8D4Ux.secret_repos`)
  ORDER BY m.max_reqs DESC
)
