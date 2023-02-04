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
import { helixStatus } from '@adobe/helix-status';
import wrap from '@adobe/helix-shared-wrap';
import { cleanupHeaderValue } from '@adobe/helix-shared-utils';
import { logger } from '@adobe/helix-universal-logger';
import { Response } from '@adobe/fetch';
import bodyData from '@adobe/helix-shared-body-data';
import { execute, queryInfo } from './sendquery.js';
import { cleanRequestParams, csvify } from './util.js';

async function runExec(params, pathname, log) {
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
      pathname.replace(/\..*$/, ''),
      params.service,
      cleanRequestParams(params),
      log,
    );

    if (pathname && pathname.endsWith('.csv')) {
      return new Response(csvify(results), {
        status: 200,
        headers: {
          'content-type': 'text/csv',
          Vary: 'X-Token, X-Service',
          ...headers,
        },
      });
    }
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
  /* c8 ignore next */
  params.service = request.headers.has('x-service') ? request.headers.get('x-service') : undefined;

  params.GOOGLE_CLIENT_EMAIL = context.env.GOOGLE_CLIENT_EMAIL;
  params.GOOGLE_PRIVATE_KEY = context.env.GOOGLE_PRIVATE_KEY;
  params.GOOGLE_PROJECT_ID = context.env.GOOGLE_PROJECT_ID;

  return runExec(params, pathname.split('/').pop(), context.log);
}

/**
 * Main function called by the openwhisk invoker.
 * @param params Action params
 * @returns {Promise<*>} The response
 */
export const main = wrap(run)
  .with(helixStatus, {
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
