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

function myTrackerApi(myCdapUrl, $resource, myAuth, myHelpers) {
  var url = myCdapUrl.constructUrl,
      searchPath = '/namespaces/:namespace/metadata/search?limit=25&target=dataset',
      basePath = '/namespaces/:namespace/:entityType/:entityId',
      programPath = '/namespaces/:namespace/apps/:appId/:programType/:programId/runs/:runId',
      propertyPath = '/namespaces/:namespace/:entityType/:entityId/metadata/properties',

      exploreQueryPath = '/namespaces/:namespace/data/explore/queries',
      baseQueryPath = '/data/explore/queries/:handle',
      // tagsPath = '/namespaces/:namespace/apps/' + CDAP_UI_CONFIG.tracker.appId + '/services/' + CDAP_UI_CONFIG.tracker.serviceId + '/methods/v1/tags';
      tagsPath = `${basePath}/metadata/tags`;

  return $resource(
    url({ _cdapPath: searchPath }),
  {
    namespace: '@namespace'
  },
  {
    search: myHelpers.getConfig('GET', 'REQUEST', searchPath, false),
    properties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/metadata?responseFormat=v6', false),
    getLineage: myHelpers.getConfig('GET', 'REQUEST', basePath + '/lineage?collapse=access&collapse=run&collapse=component'),
    getProgramRunStatus: myHelpers.getConfig('GET', 'REQUEST', programPath),
    getDatasetDetail: myHelpers.getConfig('GET', 'REQUEST', '/namespaces/:namespace/data/datasets/:entityId'),

    // USER AND PREFERRED TAGS
    getUserTags: myHelpers.getConfig('GET', 'REQUEST', `${tagsPath}?scope=USER&responseFormat=v6`, false, { suppressErrors: true }),
    deleteTag: myHelpers.getConfig('DELETE', 'REQUEST', `${tagsPath}/:tag`, false, { suppressErrors: true }),
    addTag: myHelpers.getConfig('POST', 'REQUEST', tagsPath, false, { suppressErrors: true }),

    // METADATA PROPERTIES CONTROL
    getDatasetProperties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/metadata/properties?&responseFormat=v6', false, { suppressErrors: true }),
    deleteEntityProperty: myHelpers.getConfig('DELETE', 'REQUEST', propertyPath + '/:key', false, { suppressErrors: true }),
    addEntityProperty: myHelpers.getConfig('POST', 'REQUEST', propertyPath, false, { suppressErrors: true }),

    // EXPLORE QUERY
    postQuery: myHelpers.getConfig('POST', 'REQUEST', exploreQueryPath),
    getQueryResults: myHelpers.getConfig('POST', 'REQUEST', baseQueryPath + '/next', true),
    getQuerySchema: myHelpers.getConfig('GET', 'REQUEST', baseQueryPath + '/schema', true)
  });
}

angular.module(PKG.name + '.feature.tracker')
  .factory('myTrackerApi', myTrackerApi);
