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
const fs = require('fs-extra');
const path = require('path');

function loadQuery(query) {
  return fs.readFileSync(path.resolve(__dirname, 'queries', `${query.replace(/^\//, '')}.sql`)).toString();
}

function cleanQueryParams(query, params) {
  return Object.keys(params)
    .filter((key) => query.match(new RegExp(`\\^${key}`, 'g')) == null)
    .filter((key) => query.match(new RegExp(`\\@${key}`, 'g')) != null)
    .reduce((cleanObj, key) => {
      // eslint-disable-next-line no-param-reassign
      cleanObj[key] = params[key];
      return cleanObj;
    }, {});
}

function queryReplace(query, params) {
  let outQuery = query;

  Object.keys(params)
    .filter((key) => typeof params[key] === 'string')
    .forEach((key) => {
      const regex = new RegExp(`\\^${key}`, 'g');
      // eslint-disable-next-line no-param-reassign
      params[key] = params[key].replace(/`|'|"/g, '');
      outQuery = outQuery.replace(regex, `\`${params[key]}\``);
    });
  return outQuery;
}

function getExtraParameters(query) {
  return query.split('\n')
    .filter((e) => e.startsWith('---'))
    .filter((e) => e.indexOf(':') > 0)
    .map((e) => e.substring(4).split(': '))
    .reduce((acc, val) => {
      // eslint-disable-next-line prefer-destructuring
      acc[val[0]] = val[1];
      return acc;
    }, {});
}

function cleanRequestParams(params) {
  return Object.keys(params)
    .filter((key) => !key.match(/[A-Z0-9_]+/))
    .filter((key) => !key.startsWith('__'))
    .reduce((cleanedobj, key) => {
      // eslint-disable-next-line no-param-reassign
      cleanedobj[key] = params[key];
      return cleanedobj;
    }, {});
}

module.exports = {
  loadQuery,
  getExtraParameters,
  cleanRequestParams,
  cleanQueryParams,
  queryReplace,
};
