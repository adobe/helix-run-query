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
import {
  cleanRequestParams, csvify, sshonify, extractQueryPath, chartify,
} from './util.js';

async function runExec(params, pathname, log) {
  try {
    if (pathname && pathname.endsWith('.txt')) {
      return queryInfo(pathname, params);
    }
    const {
      results, truncated, headers, description, requestParams, responseDetails,
    } = await execute(
      params.GOOGLE_CLIENT_EMAIL,
      params.GOOGLE_PRIVATE_KEY,
      params.GOOGLE_PROJECT_ID,
      pathname.replace(/\..*$/, ''),
      undefined, // service parameter is no longer used
      cleanRequestParams(params),
      log,
    );

    if (pathname && pathname.endsWith('.csv')) {
      return new Response(csvify(results), {
        status: 200,
        headers: {
          'content-type': 'text/csv',
          ...headers,
        },
      });
    }
    if (pathname && pathname.endsWith('.chart')) {
      const chartjson = chartify(results, description, params);
      const urlparams = ['width', 'height', 'devicePixelRatio', 'backgroundColor', 'format', 'version']
        .filter((param) => params[param])
        .reduce((acc, param) => {
          acc.set(param, params[param]);
          return acc;
        }, new URLSearchParams());
      urlparams.set('chart', chartjson);
      const charturl = new URL('https://quickchart.io/chart');
      charturl.search = urlparams.toString();
      return new Response(chartjson, {
        status: 307,
        headers: {
          'content-type': 'text/plain',
          ...headers,
          location: charturl.toString(),
        },
      });
    }
    delete requestParams.domainkey; // don't leak the domainkey
    return new Response(sshonify(
      results,
      description,
      requestParams,
      responseDetails,
      truncated,
    ), {
      status: 200,
      headers: {
        'content-type': 'application/json',
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
  /* c8 ignore next */
  params.domainkey = request.headers.has('authorization') ? request.headers.get('authorization').split(' ').pop() : params.domainkey || 'secret';

  params.GOOGLE_CLIENT_EMAIL = context.env.GOOGLE_CLIENT_EMAIL;
  params.GOOGLE_PRIVATE_KEY = context.env.GOOGLE_PRIVATE_KEY;
  params.GOOGLE_PROJECT_ID = context.env.GOOGLE_PROJECT_ID;

  // nested folder support
  return runExec(params, extractQueryPath(pathname), context.log);
}

/**
 * Main function called by the openwhisk invoker.
 * @param params Action params
 * @returns {Promise<*>} The response
 */
export const main = wrap(run)
  .with(helixStatus, {
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
