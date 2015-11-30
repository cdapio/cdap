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

angular.module(PKG.name + '.feature.datasets')
  .controller('DatasetsDetailController', function($scope, $state, MyCDAPDataSource, $alert, $filter, myDatasetApi, explorableDatasets) {
    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    this.explorable = explorableDatasets;

    this.truncate = function() {
      params.datasetId = $state.params.datasetId;
      myDatasetApi.truncate(params, {})
        .$promise
        .then(function () {
          $alert({
            content: 'Succesfully truncated ' + $state.params.datasetId + ' dataset',
            type: 'success'
          });
        });
    };

    this.metadataParams = {
      namespace: $state.params.namespace,
      datasetId: $state.params.datasetId
    };
    
  });
