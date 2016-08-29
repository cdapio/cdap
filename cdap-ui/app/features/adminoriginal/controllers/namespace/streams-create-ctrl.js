/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceStreamsCreateController', function($scope, $uibModalInstance, caskFocusManager, $stateParams, myStreamApi, myAlertOnValium, $state) {

    caskFocusManager.focus('streamId');

    $scope.isSaving = false;
    $scope.streamId = '';

    $scope.createStream = function() {
      if ($scope.isSaving) {
        return;
      }
      $scope.isSaving = true;

      var params = {
        namespace: $stateParams.nsadmin,
        streamId: $scope.streamId,
        scope: $scope
      };
      myStreamApi.create(params, {})
        .$promise
        .then(function() {
          $scope.isSaving = false;

          $state.go('admin.namespace.detail.data', {}, { reload: true })
            .then(function () {
              myAlertOnValium.show({
                type: 'success',
                content: 'Stream ' + $scope.streamId + ' created successfully'
              });
            });

        }, function (err) {
          $scope.isSaving = false;
          $scope.error = err.data;
        });
    };

    $scope.closeModal = function() {
      $uibModalInstance.close();
    };

    $scope.enter = function (event) {
      if (event.keyCode === 13) {
        $scope.createStream();
      }
    };

  });
