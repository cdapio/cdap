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
      searchPath = '/namespaces/:namespace/metadata/search?target=stream&target=dataset&target=view',
      basePath = '/namespaces/:namespace/:entityType/:entityId',
      programPath = '/namespaces/:namespace/apps/:appId/:programType/:programId/runs/:runId',
      auditPath = '/namespaces/:namespace/apps/Tracker/services/AuditLog/methods/auditlog/:entityType/:entityId',
      navigatorPath = '/namespaces/:namespace/apps/_ClouderaNavigator';

  return $resource(
    url({ _cdapPath: searchPath }),
  {
    namespace: '@namespace'
  },
  {
    search: myHelpers.getConfig('GET', 'REQUEST', searchPath, true),
    properties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/metadata', true),
    viewsProperties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/views/:viewId/metadata', true),
    getLineage: myHelpers.getConfig('GET', 'REQUEST', basePath + '/lineage?collapse=access&collapse=run&collapse=component'),
    getProgramRunStatus: myHelpers.getConfig('GET', 'REQUEST', programPath),
    getAuditLogs: myHelpers.getConfig('GET', 'REQUEST', auditPath),
    getStreamProperties: myHelpers.getConfig('GET', 'REQUEST', '/namespaces/:namespace/streams/:entityId'),
    getDatasetSystemProperties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/metadata/properties?scope=SYSTEM'),
    getDatasetDetail: myHelpers.getConfig('GET', 'REQUEST', '/namespaces/:namespace/data/datasets/:entityId'),
    deployNavigator: myHelpers.getConfig('PUT', 'REQUEST', navigatorPath, false, { contentType: 'application/json' }),
    getCDAPConfig: myHelpers.getConfig('GET', 'REQUEST', '/config/cdap', true),
    getNavigatorApp: myHelpers.getConfig('GET', 'REQUEST', navigatorPath, false, { suppressErrors: true }),
    getNavigatorStatus: myHelpers.getConfig('GET', 'REQUEST', navigatorPath + '/flows/MetadataFlow/runs', true),
    toggleNavigator: myHelpers.getConfig('POST', 'REQUEST', navigatorPath + '/flows/MetadataFlow/:action', false)
  });
}

angular.module(PKG.name + '.feature.tracker')
  .factory('myTrackerApi', myTrackerApi);
