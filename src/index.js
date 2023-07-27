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
import { cleanRequestParams, csvify, sshonify } from './util.js';

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
    delete requestParams.domainkey; // don't leak the domainkey
    return new Response(sshonify(
      results,
      description,
      requestParams,
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
  // the following pathname patterns all work correctly with the regular expression
  // /helix-services/run-query/file
  // /helix-services/run-query@v1/file
  // /helix-services/run-query@v2/folder/file
  // /helix-services/run-query@ci123/file
  // /helix-services/run-query@ci456/folder/file
  // /helix-services/run-query/ci789/file
  // /helix-services/run-query/ci123/folder/file
  // /helix-services/run-query@v3/folder/folder/file
  return runExec(params, pathname.replace(/^\/helix-services\/run-query((@|\/)(ci|v)\d+)*\//g, ''), context.log);
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
