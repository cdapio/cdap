/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

export const apiCreator = createApi;
export const apiCreatorAbsPath = createApiFromExactPath;
import cookie from 'react-cookie';

function createApi (dataSrc, method, type, path, options = {}) {
  return (params = {}, body) => {

    let url = buildUrl(path, params);

    let reqObj = Object.assign({ _cdapPath: url, method }, options);
    if (body) {
      reqObj = Object.assign({}, reqObj, { body });
    }

    if(cookie.load('CDAP_Auth_Token')) {
      reqObj.headers = reqObj.headers || {};
      reqObj.headers.Authorization = `Bearer ${cookie.load('CDAP_Auth_Token')}`;
    }

    if (type === 'REQUEST') {
      return dataSrc.request(reqObj);
    } else if (type === 'POLL') {
      return dataSrc.poll(reqObj);
    }
  };
}

/* The following function might be able to be merged to createApi */
function createApiFromExactPath (dataSrc, method, type, path, options = {}) {
  return (params = {}, body) => {
    let url = buildUrl(path, params);

    let reqObj = Object.assign({ url, method }, options);
    if (body) {
      reqObj = Object.assign({}, reqObj, { body });
    }

    if (type === 'REQUEST') {
      return dataSrc.request(reqObj);
    } else if (type === 'POLL') {
      return dataSrc.poll(reqObj);
    }
  };
}

function buildUrl(url, params = {}) {
  let queryParams = {};
  for (let key in params) {
    let val = params[key];

    let regexp = new RegExp(':' + key + '(\\W|$)', 'g');
    if (regexp.test(url)) {
      url = url.replace(regexp, function(match, p1) {
        return val + p1;
      });
    } else {
      queryParams[key] = val;
    }
  }

  url = addQueryParams(url, queryParams);

  return url;
}

function addQueryParams(url, params = {}) {
  if (!params) {
    return url;
  }
  var parts = [];

  function forEachSorted(obj, iterator, context) {
    var keys = Object.keys(params).sort();
    keys.forEach((key) => {
      iterator.call(context, obj[key], key);
    });
    return keys;
  }

  function encodeUriQuery(val, pctEncodeSpaces) {
    return encodeURIComponent(val).
           replace(/%40/gi, '@').
           replace(/%3A/gi, ':').
           replace(/%24/g, '$').
           replace(/%2C/gi, ',').
           replace(/%3B/gi, ';').
           replace(/%20/g, (pctEncodeSpaces ? '%20' : '+'));
  }

  forEachSorted(params, function(value, key) {
    if (value === null || typeof value === 'undefined') {
      return;
    }
    if (!Array.isArray(value)) {
      value = [value];
    }

    value.forEach((v) => {
      if (typeof v === 'object' && v !== null) {
        if (value.toString() === '[object Date]') {
          v = v.toISOString();
        } else {
          v = JSON.stringify(v);
        }
      }
      parts.push(encodeUriQuery(key) + '=' + encodeUriQuery(v));
    });
  });
  if (parts.length > 0) {
    url += ((url.indexOf('?') === -1) ? '?' : '&') + parts.join('&');
  }
  return url;
}
