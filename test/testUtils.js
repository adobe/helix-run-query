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

/* eslint-env mocha */
import assert from 'assert';
import {
  cleanHeaderParams,
  cleanQuery, cleanRequestParams, csvify,
  getHeaderParams,
  loadQuery, resolveParameterDiff,
  sshonify,
} from '../src/util.js';

describe('testing util functions', () => {
  it('loadQuery loads a query', async () => {
    const result = await loadQuery('rum-dashboard');
    assert.ok(result.match(/select/i));
  });

  it('loadQuery throws with bad query file', async () => {
    const EXPECTED = new Error('Failed to load .sql file');
    const handle = () => loadQuery('Does not Exist');
    assert.rejects(handle, EXPECTED);
    try {
      await loadQuery('Does not Exist');
    } catch (e) {
      assert.equal(e.statusCode, 404);
    }
  });

  it('query parameters are processed', () => {
    const fakeQuery = '--- helix-param: helix\n--- helix-param2: helix2\n--- helix-param3: helix3\n# this query is intentionally broken.';
    const EXPECTED = { 'helix-param': 'helix', 'helix-param2': 'helix2', 'helix-param3': 'helix3' };
    const ACTUAL = getHeaderParams(fakeQuery);
    assert.deepEqual(EXPECTED, ACTUAL);
  });

  it('query parameters are cleaned from query', () => {
    const fakeQuery = `--- helix-param: helix
--- helix-param2: helix2
--- helix-param3: helix3
#This is A random Comment
SELECT req_url, count(req_http_X_CDN_Request_ID) AS visits, resp_http_Content_Type, status_code
    FROM ^tablename
    WHERE 
      resp_http_Content_Type LIKE "text/html%" AND
      status_code LIKE "404"
    GROUP BY
      req_url, resp_http_Content_Type, status_code 
    ORDER BY visits DESC
    LIMIT @limit`;

    const EXPECTED = `SELECT req_url, count(req_http_X_CDN_Request_ID) AS visits, resp_http_Content_Type, status_code
    FROM ^tablename
    WHERE 
      resp_http_Content_Type LIKE "text/html%" AND
      status_code LIKE "404"
    GROUP BY
      req_url, resp_http_Content_Type, status_code 
    ORDER BY visits DESC
    LIMIT @limit`;
    const ACTUAL = cleanQuery(fakeQuery);
    assert.equal(EXPECTED, ACTUAL);
  });

  it('resolveParameterDiff fills in empty params with defaults', () => {
    const query = `--- something1: Likes
--- something2: CMS
--- tablename: fakeTable
--- rising: true
--- falling: false
SELECT @something1, @something2 WHERE @tablename`;
    const defaults = getHeaderParams(query);

    const params = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    const ACTUAL = resolveParameterDiff(params, defaults);

    const EXPECTED = {
      tablename: '`Helix',
      something1: '\'Loves',
      something2: 'CMS',
      rising: true,
      falling: false,
    };

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('resolveParameterDiff works if some defaults missing', () => {
    const query = '--- something1: Likes\n--- tablename: fakeTable\nSELECT @something1, @something2 WHERE @tablename';
    const defaults = getHeaderParams(query);

    const params = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    const ACTUAL = resolveParameterDiff(params, defaults);

    const EXPECTED = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('resolveParameterDiff works if all defaults missing', () => {
    const query = 'SELECT @something1, @something2 WHERE @tablename';
    const defaults = getHeaderParams(query);

    const params = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    const ACTUAL = resolveParameterDiff(params, defaults);

    const EXPECTED = {
      tablename: '`Helix',
      something1: '\'Loves',
    };

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('cleanHeaderParams removes query parameters', () => {
    const query = '--- something1: Likes\n--- something2: CMS\n--- tablename: fakeTable\nSELECT @something1, @something2 WHERE @tablename';
    const defaultParams = getHeaderParams(query);

    const ACTUAL = cleanHeaderParams(query, defaultParams, true);
    const EXPECTED = {};

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('cleanHeaderParams removes everything except Headers', () => {
    const query = '--- Cache-Control: max-age=300\n--- something1: Likes\n--- something2: CMS\n--- tablename: fakeTable\nSELECT @something1, @something2 WHERE @tablename';
    const defaultParams = getHeaderParams(query);

    const ACTUAL = cleanHeaderParams(query, defaultParams, true);
    const EXPECTED = { 'Cache-Control': 'max-age=300' };

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('cleanHeaderParams has no headers or default parameters does not fail', () => {
    const query = 'SELECT @something1, @something2 WHERE @tablename';
    const defaultParams = getHeaderParams(query);

    const ACTUAL = cleanHeaderParams(query, defaultParams);
    const EXPECTED = {};

    assert.deepEqual(ACTUAL, EXPECTED);
  });

  it('cleanRequestParams returns object', () => {
    const result = cleanRequestParams({});
    assert.equal(typeof result, 'object');
    assert.ok(!Array.isArray(result));
  });

  it('cleanRequestParams returns clean object', () => {
    const result = cleanRequestParams({
      FOOBAR: 'ahhhh',
      foobar: 'good',
      __foobar: 'bad',
    });
    assert.deepStrictEqual(result, {
      foobar: 'good',
    });
  });

  it('csvify generates csv', () => {
    const result = csvify([
      { string: 'string', bool: true, num: 1.0 },
      { string: 'str,ong', bool: false, num: -0.1 },
      { string: 'str"ong', bool: false, num: -0.1 },
    ]);
    const expected = `string,bool,num
"string",TRUE,1
"str,ong",FALSE,-0.1
"str""ong",FALSE,-0.1`;
    assert.equal(result, expected);
  });

  it('csvify generates empty csv from empty data', () => {
    const result = csvify([]);
    const expected = '';
    assert.equal(result, expected);
  });
});

describe('Test SSHONify', () => {
  it('SSHONify generates SSHON', () => {
    const input = {
      results: [{
        checkpoint: 'viewblock', source: '.article-header', ids: 3258, pages: 1031, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '32580', actions: '32580', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.images', ids: 2100, pages: 651, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '21000', actions: '21000', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.banner', ids: 1350, pages: 294, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13500', actions: '13500', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tags', ids: 1311, pages: 383, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13110', actions: '13110', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.recommended-articles', ids: 1117, pages: 271, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '11170', actions: '11170', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.embed', ids: 769, pages: 226, topurl: 'https://blog.adobe.com/en/publish/2023/04/18/new-adobe-lightroom-ai-innovations-empower-everyone-edit-like-pro', views: '7690', actions: '7690', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.animation', ids: 473, pages: 55, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4730', actions: '4730', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.featured-article', ids: 444, pages: 36, topurl: 'https://blog.adobe.com/', views: '4440', actions: '4440', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.video', ids: 420, pages: 24, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4200', actions: '4200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.article-feed', ids: 359, pages: 105, topurl: 'https://blog.adobe.com/en/topics/spark', views: '3590', actions: '3590', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.tag-header', ids: 222, pages: 63, topurl: 'https://blog.adobe.com/en/topics/spark', views: '2220', actions: '2220', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.marquee', ids: 179, pages: 55, topurl: 'https://blog.adobe.com/jp/publish/2023/03/23/cc-video-premierepro-interview-guts100tv?trackingid=RYGDMVVQ&mv=email', views: '1790', actions: '1790', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.table-of-contents', ids: 153, pages: 66, topurl: 'https://blog.adobe.com/en/publish/2023/03/21/responsible-innovation-age-of-generative-ai', views: '1530', actions: '1530', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-attr', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.error-text', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.pull-quote', ids: 47, pages: 27, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '470', actions: '470', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.author-header', ids: 38, pages: 28, topurl: 'https://blog.adobe.com/en/authors/adobe-communications-team', views: '380', actions: '380', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.toolkit', ids: 27, pages: 5, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '270', actions: '270', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.newsletter-modal', ids: 26, pages: 16, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '260', actions: '260', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image', ids: 20, pages: 7, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '200', actions: '200', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '#tabs-0', ids: 15, pages: 1, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '150', actions: '150', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infographic', ids: 14, pages: 11, topurl: 'https://blog.adobe.com/en/publish/2023/03/23/practicing-digital-accessibility-in-workplace1', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.columns', ids: 14, pages: 2, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '140', actions: '140', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.infograph', ids: 10, pages: 4, topurl: 'https://blog.adobe.com/en/publish/2022/07/27/day-in-the-life-of-a-3d-designer', views: '100', actions: '100', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.art-culos-recomendados', ids: 5, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2023/04/17/reimaginando-video-audio-adobe-firefly', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.social-links', ids: 5, pages: 5, topurl: 'https://blog.adobe.com/en/authors/cooper-savage', views: '50', actions: '50', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.carousel', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/03/08/expanding-adobe-presence-commitments-san-jose', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.namecard', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/jp/publish/2018/04/26/dtp-illustrator-kihon-tips-10', views: '30', actions: '30', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.image-50', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2021/11/24/tarjetas-de-cumpleanos-disenadas-por-heylovelygirl-para-descargar-gratis', views: '20', actions: '20', actions_per_view: '1',
      }, {
        checkpoint: 'viewblock', source: '.full-width-banner', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/02/24/academy-award-recognizes-substance-3d-for-transforming-movie-visual-effect-animation', views: '20', actions: '20', actions_per_view: '1',
      }],
      description: 'Get popularity data for RUM source attribute values, filtered by checkpoint',
      requestParams: {
        limit: 30, interval: 1, offset: '0', url: 'blog.adobe.com', checkpoint: 'viewblock', source: '-',
      },
      truncated: false,
    };
    const sshon = sshonify(input.results, input.description, input.requestParams, input.truncated);
    assert.deepStrictEqual(
      JSON.parse(sshon),
      {
        ':names': [
          'results',
          'meta',
        ],
        ':type': 'multi-sheet',
        ':version': 3,
        meta: {
          data: [
            {
              name: 'description',
              type: 'query description',
              value: 'Get popularity data for RUM source attribute values, filtered by checkpoint',
            },
            {
              name: 'limit',
              type: 'request parameter',
              value: 30,
            },
            {
              name: 'interval',
              type: 'request parameter',
              value: 1,
            },
            {
              name: 'offset',
              type: 'request parameter',
              value: '0',
            },
            {
              name: 'url',
              type: 'request parameter',
              value: 'blog.adobe.com',
            },
            {
              name: 'checkpoint',
              type: 'request parameter',
              value: 'viewblock',
            },
            {
              name: 'source',
              type: 'request parameter',
              value: '-',
            },
          ],
          columns: [
            'name',
            'value',
            'type',
          ],
          limit: 7,
          offset: 0,
          total: 7,
        },
        results: {
          limit: 30,
          offset: '0',
          total: '0',
          columns: [
            'checkpoint',
            'source',
            'ids',
            'pages',
            'topurl',
            'views',
            'actions',
            'actions_per_view',

          ],
          data: [{
            checkpoint: 'viewblock', source: '.article-header', ids: 3258, pages: 1031, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '32580', actions: '32580', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.images', ids: 2100, pages: 651, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '21000', actions: '21000', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.banner', ids: 1350, pages: 294, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13500', actions: '13500', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.tags', ids: 1311, pages: 383, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '13110', actions: '13110', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.recommended-articles', ids: 1117, pages: 271, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '11170', actions: '11170', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.embed', ids: 769, pages: 226, topurl: 'https://blog.adobe.com/en/publish/2023/04/18/new-adobe-lightroom-ai-innovations-empower-everyone-edit-like-pro', views: '7690', actions: '7690', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.animation', ids: 473, pages: 55, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4730', actions: '4730', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.featured-article', ids: 444, pages: 36, topurl: 'https://blog.adobe.com/', views: '4440', actions: '4440', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.video', ids: 420, pages: 24, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/reimagining-video-audio-adobe-firefly', views: '4200', actions: '4200', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.article-feed', ids: 359, pages: 105, topurl: 'https://blog.adobe.com/en/topics/spark', views: '3590', actions: '3590', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.tag-header', ids: 222, pages: 63, topurl: 'https://blog.adobe.com/en/topics/spark', views: '2220', actions: '2220', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.marquee', ids: 179, pages: 55, topurl: 'https://blog.adobe.com/jp/publish/2023/03/23/cc-video-premierepro-interview-guts100tv?trackingid=RYGDMVVQ&mv=email', views: '1790', actions: '1790', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.table-of-contents', ids: 153, pages: 66, topurl: 'https://blog.adobe.com/en/publish/2023/03/21/responsible-innovation-age-of-generative-ai', views: '1530', actions: '1530', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.error-attr', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.error-text', ids: 65, pages: 44, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '650', actions: '650', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.pull-quote', ids: 47, pages: 27, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '470', actions: '470', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.author-header', ids: 38, pages: 28, topurl: 'https://blog.adobe.com/en/authors/adobe-communications-team', views: '380', actions: '380', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.toolkit', ids: 27, pages: 5, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '270', actions: '270', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.newsletter-modal', ids: 26, pages: 16, topurl: 'https://blog.adobe.com/en/2019/05/30/the-future-of-adobe-air', views: '260', actions: '260', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.image', ids: 20, pages: 7, topurl: 'https://blog.adobe.com/en/publish/2023/04/17/national-volunteer-week', views: '200', actions: '200', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '#tabs-0', ids: 15, pages: 1, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '150', actions: '150', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.infographic', ids: 14, pages: 11, topurl: 'https://blog.adobe.com/en/publish/2023/03/23/practicing-digital-accessibility-in-workplace1', views: '140', actions: '140', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.columns', ids: 14, pages: 2, topurl: 'https://blog.adobe.com/en/topics/adobe-life', views: '140', actions: '140', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.infograph', ids: 10, pages: 4, topurl: 'https://blog.adobe.com/en/publish/2022/07/27/day-in-the-life-of-a-3d-designer', views: '100', actions: '100', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.art-culos-recomendados', ids: 5, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2023/04/17/reimaginando-video-audio-adobe-firefly', views: '50', actions: '50', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.social-links', ids: 5, pages: 5, topurl: 'https://blog.adobe.com/en/authors/cooper-savage', views: '50', actions: '50', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.carousel', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/03/08/expanding-adobe-presence-commitments-san-jose', views: '30', actions: '30', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.namecard', ids: 3, pages: 1, topurl: 'https://blog.adobe.com/jp/publish/2018/04/26/dtp-illustrator-kihon-tips-10', views: '30', actions: '30', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.image-50', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/es/publish/2021/11/24/tarjetas-de-cumpleanos-disenadas-por-heylovelygirl-para-descargar-gratis', views: '20', actions: '20', actions_per_view: '1',
          }, {
            checkpoint: 'viewblock', source: '.full-width-banner', ids: 2, pages: 1, topurl: 'https://blog.adobe.com/en/publish/2023/02/24/academy-award-recognizes-substance-3d-for-transforming-movie-visual-effect-animation', views: '20', actions: '20', actions_per_view: '1',
          }],
        },
      },

    );
  });
});
