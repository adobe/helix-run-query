#!/usr/bin/env node
/*
 * Copyright 2023 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
// eslint-disable-next-line import/no-extraneous-dependencies
import YAML from 'yaml';
import path from 'path';
import { loadQuery, getHeaderParams, getTrailingParams } from '../src/util.js';

function camelCase(str) {
  return str.replace(/-([a-z])/g, (g) => g[1].toUpperCase());
}

function coerce(value) {
  if (value === 'true') {
    return true;
  } else if (value === 'false') {
    return false;
  }
  // integers
  if (Number.isInteger(Number(value))) {
    return Number(value);
  }
  // floats
  if (Number.isFinite(Number(value))) {
    return Number(value);
  }
  return value;
}

function schematype(value) {
  // integers
  if (Number.isInteger(Number(value))) {
    return 'integer';
  }
  return typeof coerce(value);
}

// input query file
const query = process.argv[2];
const queryName = path.basename(query).replace(/\.sql$/, '');

// eslint-disable-next-line no-underscore-dangle
global.__rootdir = path.resolve(process.cwd());
const queryContent = await loadQuery(queryName);
const headerParams = getHeaderParams(queryContent);
const trailingParams = getTrailingParams(queryContent);

const defaultParams = ['interval', 'offset', 'url', 'limit', 'domainkey', 'startdate', 'enddate', 'timezone'];

function mapHeaderParams(params) {
  return Object.entries(params)
    // exclude description
    .filter(([key]) => key !== 'description')
    // exclude parameters starting with a capital letter
    .filter(([key]) => key[0] === key[0].toLowerCase())
    .map(([key, value]) => {
      // if the key is in the default params, then we return the default definition
      if (defaultParams.includes(key)) {
        return {
          $ref: `../parameters.yaml#/${camelCase(`queryDefault-${key}QueryParam`)}`,
        };
      }
      return {
        name: key,
        in: 'query',
        default: coerce(value),
        type: schematype(value),
      };
    });
}

function mapTrailingParams(params) {
  if (Object.keys(params).length === 0) {
    return {
      // we don't know what will be returned, so we allow anything
      additionalProperties: true,
    };
  }
  return {
    properties: Object.entries(params)
      .reduce((obj, [key, value]) => {
        // eslint-disable-next-line no-param-reassign
        obj[key] = {
          description: value,
          type: 'string',
        };
        return obj;
      }, {}),
  };
}

const yaml = {
  query: {
    get: {
      operationId: camelCase(`get-${queryName}`),
      tags: ['query'],
      summary: headerParams.description,
      parameters: [...mapHeaderParams(headerParams), {
        $ref: '../parameters.yaml#/queryExtensionQueryParam',
      },
      {
        $ref: '../parameters.yaml#/headerDomainkeyAuthorizationParam',
      }],
      responses: {
        200: {
          $ref: '#200response',
        },
        307: {
          $ref: '#307response',
        },
        400: {
          $ref: '../responses.yaml#400',
        },
        401: {
          $ref: '../responses.yaml#401',
        },
        403: {
          $ref: '../responses.yaml#403',
        },
        404: {
          $ref: '../responses.yaml#404',
        },
        500: {
          $ref: '../responses.yaml#500',
        },
      },
    },
    post: {
      operationId: camelCase(`post-${queryName}`),
      tags: ['query'],
      summary: headerParams.description,
      parameters: [...mapHeaderParams(headerParams), {
        $ref: '../parameters.yaml#/queryExtensionQueryParam',
      },
      {
        $ref: '../parameters.yaml#/headerDomainkeyAuthorizationParam',
      }],
      responses: {
        200: {
          $ref: '#200response',
        },
        307: {
          $ref: '#307response',
        },
        400: {
          $ref: '../responses.yaml#400',
        },
        401: {
          $ref: '../responses.yaml#401',
        },
        403: {
          $ref: '../responses.yaml#403',
        },
        404: {
          $ref: '../responses.yaml#404',
        },
        500: {
          $ref: '../responses.yaml#500',
        },
      },
    },
  },
  '200response': {
    content: {
      'text/csv': {
        description: 'Response as a CSV file. This will be returned if the extension ".csv" has been provided.',
        schema: 'string',
        format: 'binary',
      },
      'application/json': {
        description: 'Response as JSON in Spreadsheet Object Notation (SSHON). This will be returned if no extension has been provided.',
        schema: 'object',
        properties: {
          ':names': {
            description: 'Names of the sheets in the result. Always contains "results" and "meta".',
            type: 'array',
            const: ['results', 'meta'],
          },
          ':type': {
            description: 'Type of the result. Always "multi-sheet".',
            type: 'string',
            const: 'multi-sheet',
          },
          ':version': {
            description: 'Version of the result. Always 3.',
            type: 'integer',
            const: 3,
          },
          results: {
            description: 'The result sheet, containing metadata about the size of the result set and the actual results',
            type: 'object',
            properties: {
              limit: {
                description: 'The maximum number of results returned.',
                type: 'integer',
                minimum: 0,
              },
              offset: {
                description: 'The offset of the first result returned.',
                type: 'integer',
                minimum: 0,
              },
              total: {
                description: 'The total number of rows in the result set',
                type: 'integer',
                minimum: 0,
              },
              data: {
                description: 'The actual results',
                type: 'array',
                items: {
                  description: 'A row of the result set',
                  type: 'object',
                  ...(mapTrailingParams(trailingParams)),
                },
              },
              columns: {
                description: 'The columns of the result set',
                type: 'array',
                items: {
                  type: 'string',
                  description: 'The name of the column',
                },
              },
            },
          },
        },
      },
    },
  },
  '307response': {
    content: {
      'text/plain': {
        description: 'Response as a chart.js chart. Response as a chart.js chart. This will be returned if the extension ".chart" has been provided.',
        schema: 'string',
        format: 'binary',
      },
    },
    headers: {
      location: {
        description: 'The URL to the chart.',
        schema: {
          type: 'string',
          format: 'uri',
        },
      },
    },
  },
};

// turn headerParams into yaml
console.log(YAML.stringify(yaml));
