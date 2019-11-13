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
const { openWhiskWrapper } = require('epsagon');
const { wrap } = require('@adobe/helix-status');
const { execute } = require('./sendquery.js');
const { cleanRequestParams } = require('./util.js');

async function runExec(params) {
  try {
    const { results, truncated } = await execute(
      params.GOOGLE_CLIENT_EMAIL,
      params.GOOGLE_PRIVATE_KEY,
      params.GOOGLE_PROJECT_ID,
      params.__ow_path,
      params.service,
      cleanRequestParams(params),
    );
    return {
      headers: {
        'content-type': 'application/json',
        Vary: 'X-Token, X-Service',
      },
      body: {
        results,
        truncated,
      },
    };
  } catch (e) {
    return {
      statusCode: e.statusCode || 500,
      body: e.message,
    };
  }
}

/**
 * Runs the action by wrapping the `runExec` function with the pingdom-status utility.
 * Additionally, if a EPSAGON_TOKEN is configured, the epsagon tracers are instrumented.
 * @param params Action params
 * @returns {Promise<*>} The response
 */
async function run(params) {
  let action = runExec;
  action = openWhiskWrapper(runExec, {
    token_param: 'EPSAGON_TOKEN',
    appName: 'Helix Services',
    metadataOnly: false, // Optional, send more trace data
    ignoredKeys: ['token', 'GOOGLE_PRIVATE_KEY', /[A-Z0-9_]+/],
  });
  return wrap(action, {
    fastly: 'https://api.fastly.com/public-ip-list',
    googleiam: 'https://iam.googleapis.com/$discovery/rest?version=v1',
    googlebigquery: 'https://www.googleapis.com/discovery/v1/apis/bigquery/v2/rest',
  })(params);
}

async function main(params) {
  if (params.__ow_headers && ('x-token' in params.__ow_headers) && ('x-service' in params.__ow_headers)) {
    // eslint-disable-next-line no-param-reassign
    params.token = params.__ow_headers['x-token'];
    // eslint-disable-next-line no-param-reassign
    params.service = params.__ow_headers['x-service'];
  }
  return run(params);
}

module.exports.main = main;
