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
const initfastly = require('@adobe/fastly-native-promises');
const fs = require('fs-extra');
const path = require('path');

/**
 *
 * @param {string} token Fastly Authentication Token
 * @param {string} service serviceid for a helix-project
 */
async function authFastly(token, service) {
  // verify Fastly credentials
  const Fastly = await initfastly(token, service);
  await Fastly.getVersions();
  return true;
}

/**
 *
 * @param {string} query name of the query file
 */
function loadQuery(query) {
  return fs.readFileSync(path.resolve(__dirname, 'queries', `${query.replace(/^\//, '')}.sql`)).toString();
}

/**
 *
 * @param {string} query the content read from a query file
 * @param {object} params query parameters, that are inserted into query
 */
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

/**
 *
 * @param {string} query the content read from a query file
 * @param {*} params query parameters, that are inserted into query
 */
function queryReplace(query, params) {
  let outQuery = query;

  Object.keys(params)
    .filter((key) => typeof params[key] === 'string')
    .forEach((key) => {
      const regex = new RegExp(`\\^${key}`, 'g');
      const sqlInjCheck = params[key].match(/[;\s]/g);
      if(sqlInjCheck != null){
        throw new Error("Only single phrase parameters allowed");
      }
      // eslint-disable-next-line no-param-reassign
      params[key] = params[key].replace(/`|'|"/g, '');
      outQuery = outQuery.replace(regex, `\`${params[key]}\``);
    });
  return outQuery;
}

/**
 *
 * @param {string} query the content read from a query file
 */
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

/**
 *
 * @param {object} params all parameters contained in a request
 */
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
  authFastly,
};
