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
  .factory('myTagsApi', function(myCdapUrl, $resource, myHelpers) {

    var url = myCdapUrl.constructUrl,
        metadataPath = '/metadata/tags',
        basePath = '/namespaces/:namespaceId',
        appPath = basePath + '/apps/:appId',
        programPath = appPath + '/:programType/:programId',
        datasetPath = basePath + '/datasets/:datasetId',
        streamsPath = basePath + '/streams/:streamId',
        searchPath = basePath + '/metadata/search';

    return $resource(
      url({ _cdapPath: basePath }),
    {
      namespaceId: '@namespaceId',
      datasetId: '@datasetId',
      streamId: '@streamId',
      programType: '@programType',
      programId: '@programId',
      appId: '@appId'
    },
    {
      getAppTags: myHelpers.getConfig('GET', 'REQUEST', appPath + metadataPath, true),
      getProgramTags: myHelpers.getConfig('GET', 'REQUEST', programPath + metadataPath, true),
      getDatasetTags: myHelpers.getConfig('GET', 'REQUEST', datasetPath + metadataPath, true),
      getStreamTags: myHelpers.getConfig('GET', 'REQUEST', streamsPath + metadataPath, true),
      searchTags: myHelpers.getConfig('GET', 'REQUEST', searchPath)
    }
  );
});
