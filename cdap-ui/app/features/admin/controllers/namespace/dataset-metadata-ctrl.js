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

angular.module(PKG.name + '.feature.admin').controller('NamespaceDatasetMetadataController',
function ($scope, $state, myAlertOnValium, $filter, myDatasetApi, myExploreApi, EventPipe) {

  var params = {
    namespace: $state.params.nsadmin,
    scope: $scope
  };

  myExploreApi.list(params)
    .$promise
    .then(function (tables) {

      var datasetId = $state.params.datasetId;
      datasetId = datasetId.replace(/[\.\-]/g, '_');

      var match = $filter('filter')(tables, datasetId);
      if (match.length > 0) {

        params.table = 'dataset_' + datasetId;

        myExploreApi.getInfo(params)
          .$promise
          .then(function (res) {
            $scope.metadata = res;
          });

      } else {
        $scope.metadata = null;
      }
    });


  $scope.deleteDataset = function() {
    EventPipe.emit('showLoadingIcon');
    var params = {
      namespace: $state.params.nsadmin,
      datasetId: $state.params.datasetId,
      scope: $scope
    };
    myDatasetApi.delete(params, {}, function success() {
      EventPipe.emit('hideLoadingIcon.immediate');

      $state.go('admin.namespace.detail.data', {}, {reload: true});
      myAlertOnValium.show({
        type: 'success',
        content: 'Successfully deleted dataset'
      });
    }, function error() {
      EventPipe.emit('hideLoadingIcon.immediate');
    });
  };

});
