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