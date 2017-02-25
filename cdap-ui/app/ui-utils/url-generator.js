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

/*
  Purpose: To generate absolute URLs to navigate between CDAP and Extensions (Hydrator & Tracker)
    as they are separate web apps with independent UI routing
  I/P: Context/Naviation object that has,
    - uiApp - cdap, hydrator or tracker
    - namespaceId, appId, entityType, entityId & runId to generate the complete URL if
      appropriate context is available.

  O/P: Absolute Url of the form:
    <protocol>//<host>/:uiApp/:namespaceId/apps/:appId/:entityType/:entityId/runs/:runId

  The absolute URL will be generated based on the available context.

  Note:
    This is attached to the window object as this needs to be used in both CDAP (react app) and
    in hydrator & tracker (angular apps). For now it is attached to window as its a pure function
    without any side effects. Moving forward once we have everything in react we should use this
    as a proper utility function in es6 module system.
*/
window.getAbsUIUrl = function(navigationObj = {}) {
  let {uiApp = 'cdap', redirectUrl, clientId, namespaceId, appId, entityType, entityId, runId} = navigationObj;
  let baseUrl = `${location.protocol}//${location.host}/${uiApp}`;
  if (uiApp === 'login') {
    baseUrl += `?`;
  }
  if (redirectUrl) {
    baseUrl += `redirectUrl=${encodeURIComponent(redirectUrl)}`;
  }
  if (clientId) {
    baseUrl += `&clientId=${clientId}`;
  }
  if (namespaceId) {
    baseUrl += `/ns/${namespaceId}`;
  }
  if (appId) {
    baseUrl += `/apps/${appId}`;
  }
  if (entityType && entityId) {
    baseUrl += `/${entityType}/${entityId}`;
  }
  if (runId) {
    baseUrl += `/runs/${runId}`;
  }
  return baseUrl;
};
function buildCustomUrl(url, params = {}) {
  let queryParams = {};
  for (let key in params) {
    if (!params.hasOwnProperty(key)) {
      continue;
    }
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

  url = addCustomQueryParams(url, queryParams);

  return url;
}
function addCustomQueryParams(url, params = {}) {
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

window.getTrackerUrl = function(navigationObj = {}) {
  let {stateName, stateParams} = navigationObj;
  let uiApp = 'metadata';
  let baseUrl = `${location.protocol}//${location.host}/${uiApp}/ns/:namespace`;
  let stateToUrlMap = {
    'tracker': '',
    'tracker.detail': '',
    'tracker.detail.entity': '/entity/:entityType/:entityId',
    'tracker.detail.entity.metadata': '/entity/:entityType/:entityId/metadata',
    'tracker.integrations': '/integrations',
    'tracker.detail.entity.audit': '/entity/:entityType/:entityId/audit',
    'tracker.detail.entity.lineage': '/entity/:entityType/:entityId/lineage',
    'tracker.detail.entity.usage': '/entity/:entityType/:entityId/usage',
    'tracker.tags': '/tags',
    'tracker.dictionary': '/dictionary'
  };
  let url = baseUrl + stateToUrlMap[stateName || 'tracker'];
  url = buildCustomUrl(url, stateParams);
  return url;
};
window.getHydratorUrl = function(navigationObj = {}) {
  let {stateName, stateParams} = navigationObj;
  let uiApp = 'pipelines';
  let baseUrl = `${location.protocol}//${location.host}/${uiApp}/ns/:namespace`;
  let stateToUrlMap = {
    'hydrator': '',
    'hydrator.create': '/studio',
    'hydrator.detail': '/view/:pipelineId',
    'hydrator.list': '?page'
  };
  let url = baseUrl + stateToUrlMap[stateName || 'pipelines'];
  url = buildCustomUrl(url, stateParams);
  return url;
};
window.getOldCDAPUrl = function(navigationObj = {}) {
  let {stateName, stateParams} = navigationObj;
  let uiApp = 'oldcdap';
  let baseUrl = `${location.protocol}//${location.host}/${uiApp}/ns/:namespace`;
  let stateToUrlMap = {
    'datasets.detail.overview.status': '/datasets/:datasetId/overview/status',
    'datasets.detail.overview.explore': '/datasets/:datasetId/overview/explore',
    'streams.detail.overview.status': '/streams/:streamId/overview/status',
    'streams.detail.overview.explore': '/streams/:streamId/overview/explore',
    'apps.detail.overview.status': '/apps/:appId/overview/status',
    'apps.detail.overview.programs': '/apps/:appId/overview/programs',
    'mapreduce.detail': '/apps/:appId/programs/mapreduce/:programId/runs',
    'workflows.detail': '/apps/:appId/programs/workflows/:programId/runs',
    'workflows.detail.run': '/apps/:appId/programs/workflows/:programId/runs/:runId',
    'workers.detail': '/apps/:appId/programs/workers/:programId/runs',
    'workers.detail.run': '/apps/:appId/programs/workers/:programId/runs/:runId',
    'spark.detail': '/apps/:appId/programs/spark/:programId/runs',
    'spark.detail.run': '/apps/:appId/programs/spark/:programId/runs/:runId',
    'flows.detail': '/apps/:appId/programs/flows/:programId/runs',
    'services.detail': '/apps/:appId/programs/services/:programId/runs'
  };
  let url = baseUrl + stateToUrlMap[stateName || 'pipelines'];
  url = buildCustomUrl(url, stateParams);
  return url;
};
