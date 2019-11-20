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
 * authenticates token and service with Fastly
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
 * reads a query file and loads it into memory
 *
 * @param {string} query name of the query file
 */
async function loadQuery(query) {
  const pathName = path.resolve(__dirname, 'queries', `${query.replace(/^\//, '')}.sql`);
  return new Promise(((resolve, reject) => {
    fs.readFile(pathName, (err, data) => {
      if (err) {
        reject(new Error('Failed to load .sql file'));
      } else {
        resolve(data.toString());
      }
    });
  }));
}

/**
 * strips params object of helix query parameters
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
 * replaces helix query parameter placeholders denoted by ^param
 * in a query with value in param object. Example; SELECT * FROM ^tablename
 * ^tabename is the query parameter; so it's expected that the params object
 * contains {tablename: 'some-value'}
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
      if (sqlInjCheck != null) {
        throw new Error('Only single phrase parameters allowed');
      }
      // eslint-disable-next-line no-param-reassign
      params[key] = params[key].replace(/`|'|"/g, '');
      outQuery = outQuery.replace(regex, `\`${params[key]}\``);
    });
  return outQuery;
}


/**
 * Processes additional parameters relating to query properties, like -- Authorization
 * and other properties that will be passed into request/response headers: for example;
 * --- Cache-Control: max-age: 300.
 *
 * @param {string} query the content read from a query file
 */
function getHeaderParams(query) {
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
 * cleans out extra parameters from query and leaves only query
 *
 * @param {string} query the content read from a query file
 */
function cleanHeaderParams(query) {
  return query.split('\n')
    .filter((e) => !e.startsWith('---'))
    .filter((e) => !e.startsWith('#'))
    .join(' ');
}

/**
 * removes used up parameters from request
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

/**
 * replaces tablename with union of tables from one dataset or multiple datasets
 * i.e:
 * if query inputted is SELECT req_url FROM ^myrequest
 * output will become SELECT req_url FROM (SELECT * FROM helix_logging_myService.requests*)
 *
 * @param {string} query a query loaded from loadQuery with placeholders
 * @param {object} replacers an function mapping from placeholders to replacer methods
 */
async function replaceTableNames(query, replacers) {
  const replacements = await (query.match(/\^[a-z]+/g) ? query.match(/\^[a-z]+/g) : [])
    .map((placeholder) => placeholder.substr(1))
    .reduce(async (pvp, placeholder) => {
      const pv = await pvp;
      if (pv[placeholder]) {
        return pv;
      }
      pv[placeholder] = await replacers[placeholder]();
      return pv;
    }, {});

  return Object.keys(replacements).reduce((q, placeholder) => q.replace(new RegExp(`\\^${placeholder}`, 'g'), replacements[placeholder]), query);
}

/**
 * combines user provided params and fills in missing from query file
 * @param {object} params provided parameters
 * @param {object} defaults default parameters in query file
 */
function resolveParameterDiff(params, defaults){
  const uniques = Object.keys(defaults)
  .filter((key) => !(key in params))
  .reduce((pv, cv) => {
    pv[cv] = defaults[cv];
    return pv;
  }, {});

  return Object.assign(uniques, params);
}

module.exports = {
  loadQuery,
  getHeaderParams,
  cleanRequestParams,
  cleanQueryParams,
  cleanHeaderParams,
  queryReplace,
  authFastly,
  replaceTableNames,
  resolveParameterDiff,
};
