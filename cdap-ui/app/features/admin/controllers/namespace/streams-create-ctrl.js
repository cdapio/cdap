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

angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceStreamsCreateController', function($scope, $modalInstance, caskFocusManager, $stateParams, myStreamApi, $alert, $timeout) {

    caskFocusManager.focus('streamId');

    $scope.isSaving = false;
    $scope.streamId = '';

    $scope.createStream = function() {
      $scope.isSaving = true;
      var params = {
        namespace: $stateParams.nsadmin,
        streamId: $scope.streamId,
        scope: $scope
      };
      myStreamApi.create(params, {})
        .$promise
        .then(function(res) {
          // FIXME: We reload on state leave. So when we show an alert
          // here and reload the state we have that flickering in the
          // alert message. In order to avoid that I am delaying by 100ms
          $timeout(function() {
            $alert({
              type: 'success',
              content: 'Stream ' + $scope.streamId + ' created successfully'
            });
          }, 100);
          $scope.isSaving = false;
          $modalInstance.close(res);
        }, function (err) {
          $scope.isSaving = false;
          $scope.error = err.data;
        });
    };

    $scope.closeModal = function() {
      $modalInstance.close();
    };

    $scope.enter = function (event) {
      if (event.keyCode === 13) {
        $scope.createStream();
      }
    };

  });
