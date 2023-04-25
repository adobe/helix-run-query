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
import fs from 'fs-extra';
import path from 'path';
import { MissingQueryError } from './missing-query-error.js';

/**
 * reads a query file and loads it into memory
 *
 * @param {string} query name of the query file
 */
export async function loadQuery(query) {
  const pathName = path.resolve(__rootdir, 'src', 'queries', `${query.replace(/^\//, '')}.sql`);
  return new Promise(((resolve, reject) => {
    fs.readFile(pathName, (err, data) => {
      if (err) {
        reject(new MissingQueryError(`Failed to load .sql file ${pathName}`));
      } else {
        resolve(data.toString('utf8'));
      }
    });
  }));
}

/**
 * strips param object of everything save headers!
 *
 * @param {string} query the content read from a query file
 * @param {object} params query parameters, that are inserted into query
 */
export function cleanHeaderParams(query, params, rmvQueryParams = false) {
  return Object.keys(params)
    .filter((key) => rmvQueryParams !== (query.match(new RegExp(`\\@${key}`, 'g')) != null))
    .filter((key) => key !== 'description')
    .reduce((cleanObj, key) => {
      // eslint-disable-next-line no-param-reassign
      cleanObj[key] = params[key];
      return cleanObj;
    }, {});
}

function coerce(value) {
  if (value === 'true') {
    return true;
  } else if (value === 'false') {
    return false;
  }
  return value;
}

/**
 * Processes additional parameters relating to query properties, like -- Authorization
 * and other properties that will be passed into request/response headers: for example;
 * --- Cache-Control: max-age: 300.
 *
 * @param {string} query the content read from a query file
 */
export function getHeaderParams(query) {
  return query.split('\n')
    .filter((e) => e.startsWith('---'))
    .filter((e) => e.indexOf(':') > 0)
    .map((e) => e.substring(4).split(': '))
    .reduce((acc, val) => {
      // eslint-disable-next-line prefer-destructuring
      acc[val[0]] = coerce(val[1]);
      return acc;
    }, {});
}

/**
 * cleans out extra parameters from query and leaves only query
 *
 * @param {string} query the content read from a query file
 */
export function cleanQuery(query) {
  return query.split('\n')
    .filter((e) => !e.startsWith('---'))
    .filter((e) => !e.startsWith('#'))
    .join('\n');
}

/**
 * removes used up parameters from request
 *
 * @param {object} params all parameters contained in a request
 */
export function cleanRequestParams(params) {
  return Object.keys(params)
    .filter((key) => !key.match(/^[A-Z0-9_]+/))
    .filter((key) => !key.startsWith('__'))
    .reduce((cleanedobj, key) => {
      // eslint-disable-next-line no-param-reassign
      cleanedobj[key] = params[key];
      return cleanedobj;
    }, {});
}

/**
 * fills in missing query parameters (if any) with defaults from query file
 * @param {object} params provided parameters
 * @param {object} defaults default parameters in query file
 */
export function resolveParameterDiff(params, defaults) {
  return Object.assign(defaults, params);
}

function format(entry) {
  switch (typeof entry) {
    case 'boolean': return String(entry).toUpperCase();
    case 'string': return `"${entry.replace(/"/g, '""')}"`;
    default: return String(entry);
  }
}

export function csvify(arr) {
  const [first = {}] = arr;
  return [
    Array.from(Object.keys(first)).join(','),
    ...arr.map((line) => Object.values(line).map(format).join(',')),
  ].join('\n');
}

/**
 * SSHON is Simple Spreadsheet Object Notation (read: Sean, like Jason), the
 * format used by Helix to serve spreadsheets. This function converts a SQL
 * result set into a SSHON string.
 * @param {object[]} results the SQL result set
 * @param {string} description the description of the query
 * @param {object} requestParams the request parameters
 * @param {boolean} truncated whether the result set was truncated
 * @returns {string} the SSHON string
 */
export function sshonify(results, description, requestParams, truncated) {
  const sson = {
    ':names': ['results', 'meta'],
    ':type': 'multi-sheet',
    ':version': 3,
    results: {
      limit: Math.max(requestParams.limit || 1, results.length),
      offset: requestParams.offset || 0,
      total: requestParams.offset || 0 + results.length + (truncated ? 1 : 0),
      data: results,
      columns: Object.keys(results[0] || {}),
    },
    meta: {
      limit: 1 + Object.keys(requestParams).length,
      offset: 0,
      total: 1 + Object.keys(requestParams).length,
      columns: ['name', 'value', 'type'],
      data: [
        {
          name: 'description',
          value: description,
          type: 'query description',
        },
        ...Object.entries(requestParams).map(([key, value]) => ({
          name: key,
          value,
          type: 'request parameter',
        })),
      ],
    },
  };
  return JSON.stringify(sson);
}
