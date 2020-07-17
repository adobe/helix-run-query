/*
 * Copyright 2019 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
const { wrap: status } = require('@adobe/helix-status');
const { wrap } = require('@adobe/openwhisk-action-utils');
const { logger } = require('@adobe/openwhisk-action-logger');
const { epsagon } = require('@adobe/helix-epsagon');
const { execute, queryInfo } = require('./sendquery.js');
const { cleanRequestParams, accessLogFormat } = require('./util.js');

async function runExec(params) {
  try {
    if (params.__ow_path && params.__ow_path.endsWith('.txt')) {
      return queryInfo(params);
    }
    const {
      results, truncated, headers, description, requestParams,
    } = await execute(
      params.GOOGLE_CLIENT_EMAIL,
      params.GOOGLE_PRIVATE_KEY,
      params.GOOGLE_PROJECT_ID,
      params.__ow_path,
      params.service,
      cleanRequestParams(params),
    );
    const result = {
      headers: {
        'content-type': 'application/json',
        Vary: 'X-Token, X-Service',
        ...headers,
      },
      body: {
        results: (params.format === 'access.log') ? results.map(accessLogFormat) : results,
        description,
        requestParams,
        truncated,
      },
    };
    return result;
  } catch (e) {
    return {
      statusCode: e.statusCode || 500,
      body: e.message,
    };
  }
}

async function run(params) {
  if (params.__ow_headers
    && ('x-token' in params.__ow_headers)
    && ('x-service' in params.__ow_headers)) {
    // eslint-disable-next-line no-param-reassign
    params.token = params.__ow_headers['x-token'];
    // eslint-disable-next-line no-param-reassign
    params.service = params.__ow_headers['x-service'];
  }
  return runExec(params);
}

/**
 * Main function called by the openwhisk invoker.
 * @param params Action params
 * @returns {Promise<*>} The response
 */
module.exports.main = wrap(run)
  .with(epsagon)
  .with(status, {
    fastly: 'https://api.fastly.com/public-ip-list',
    googleiam: 'https://iam.googleapis.com/$discovery/rest?version=v1',
    googlebigquery: 'https://www.googleapis.com/discovery/v1/apis/bigquery/v2/rest',
  })
  .with(logger.trace)
  .with(logger);
