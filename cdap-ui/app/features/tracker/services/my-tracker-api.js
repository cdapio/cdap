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
      searchPath = '/namespaces/:namespace/metadata/search',
      basePath = '/namespaces/:namespace/:entityType/:entityId',
      programPath = '/namespaces/:namespace/apps/:appId/:programType/:programId/runs/:runId';

  return $resource(
    url({ _cdapPath: searchPath }),
  {
    namespace: '@namespace'
  },
  {
    search: myHelpers.getConfig('GET', 'REQUEST', searchPath, true),
    properties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/metadata', true),
    viewsProperties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/views/:viewId/metadata', true),
    getLineage: myHelpers.getConfig('GET', 'REQUEST', basePath + '/lineage'),
    getProgramRunStatus: myHelpers.getConfig('GET', 'REQUEST', programPath)
  });
}

angular.module(PKG.name + '.feature.tracker')
  .factory('myTrackerApi', myTrackerApi);
