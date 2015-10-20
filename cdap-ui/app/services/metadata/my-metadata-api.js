/*
 * Copyright © 2015 Cask Data, Inc.
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

angular.module(PKG.name + '.services')
  .factory('myMetadataApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        programPath = '/namespaces/:namespace/apps/:appId/:programType/:programId/metadata',
        datasetsPath = '/namespaces/:namespace/datasets/:datasetId/metadata',
        streamsPath = '/namespaces/:namespace/streams/:streamId/metadata',
        appsPath = '/namespaces/:namespace/apps/:appId/metadata';

    return $resource(
      url({ _cdapPath: programPath }),
    {
      namespace: '@namespace',
      appId: '@appId',
      programType: '@programType',
      programId: '@programId',
    },
    {
      setProgramMetadata: myHelpers.getConfig('POST', 'REQUEST', programPath + '/tags'),
      getProgramMetadata: myHelpers.getConfig('GET', 'REQUEST', programPath + '/tags', true),
      deleteProgramMetadata: myHelpers.getConfig('DELETE', 'REQUEST', programPath + '/tags/:tag'),

      setAppsMetadata: myHelpers.getConfig('POST', 'REQUEST', appsPath + '/tags'),
      getAppsMetadata: myHelpers.getConfig('GET', 'REQUEST', appsPath + '/tags', true),
      deleteAppsMetadata: myHelpers.getConfig('DELETE', 'REQUEST', appsPath + '/tags/:tag'),

      setDatasetsMetadata: myHelpers.getConfig('POST', 'REQUEST', datasetsPath + '/tags'),
      getDatasetsMetadata: myHelpers.getConfig('GET', 'REQUEST', datasetsPath + '/tags', true),
      deleteDatasetsMetadata: myHelpers.getConfig('DELETE', 'REQUEST', datasetsPath + '/tags/:tag'),

      setStreamsMetadata: myHelpers.getConfig('POST', 'REQUEST', streamsPath + '/tags'),
      getStreamsMetadata: myHelpers.getConfig('GET', 'REQUEST', streamsPath + '/tags', true),
      deleteStreamsMetadata: myHelpers.getConfig('DELETE', 'REQUEST', streamsPath + '/tags/:tag'),

    });
  });
