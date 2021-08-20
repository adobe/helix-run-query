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
const wrap = require('@adobe/helix-shared-wrap');
const { cleanupHeaderValue } = require('@adobe/helix-shared-utils');
const { logger } = require('@adobe/helix-universal-logger');
const { Response } = require('@adobe/helix-universal');
const bodyData = require('@adobe/helix-shared-body-data');
const { execute, queryInfo } = require('./sendquery.js');
const { cleanRequestParams } = require('./util.js');

async function runExec(params, pathname) {
  try {
    if (pathname && pathname.endsWith('.txt')) {
      return queryInfo(pathname, params);
    }
    const {
      results, truncated, headers, description, requestParams,
    } = await execute(
      params.GOOGLE_CLIENT_EMAIL,
      params.GOOGLE_PRIVATE_KEY,
      params.GOOGLE_PROJECT_ID,
      pathname,
      params.service,
      cleanRequestParams(params),
    );

    return new Response(JSON.stringify({
      results,
      description,
      requestParams,
      truncated,
    }), {
      status: 200,
      headers: {
        'content-type': 'application/json',
        Vary: 'X-Token, X-Service',
        ...headers,
      },
    });
  } catch (e) {
    return new Response(e.message, {
      status: e.statusCode || 500,
      headers: {
        'x-error': cleanupHeaderValue(e.message),
      },
    });
  }
}

async function run(request, context) {
  const { pathname } = new URL(request.url);
  const params = context.data;
  params.token = request.headers.has('x-token') ? request.headers.get('x-token') : undefined;
  params.service = request.headers.has('x-service') ? request.headers.get('x-service') : undefined;

  params.GOOGLE_CLIENT_EMAIL = context.env.GOOGLE_CLIENT_EMAIL;
  params.GOOGLE_PRIVATE_KEY = context.env.GOOGLE_PRIVATE_KEY;
  params.GOOGLE_PROJECT_ID = context.env.GOOGLE_PROJECT_ID;

  return runExec(params, pathname.split('/').pop());
}

/**
 * Main function called by the openwhisk invoker.
 * @param params Action params
 * @returns {Promise<*>} The response
 */
module.exports.main = wrap(run)
  .with(status, {
    fastly: 'https://api.fastly.com/public-ip-list',
    googleiam: 'https://iam.googleapis.com/$discovery/rest?version=v1',
    googlebigquery: 'https://www.googleapis.com/discovery/v1/apis/bigquery/v2/rest',
  })
  .with(logger.trace)
  .with(logger)
  .with(bodyData, {
    coerceInt: true,
    coerceBoolean: true,
    coerceNumber: true,
  });
