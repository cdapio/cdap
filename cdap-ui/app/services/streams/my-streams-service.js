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

angular.module(PKG.name + '.services')
  .service('myStreamService', function($uibModal, $rootScope) {
    var modalInstance;

    this.show = function(streamId) {
			var scope = $rootScope.$new();
			scope.streamId = streamId;
      modalInstance = $uibModal.open({
        controller: 'FlowStreamDetailController',
        templateUrl: '/assets/features/flows/templates/tabs/runs/streams/detail.html',
        scope: scope
      });
      return modalInstance;
    };

    this.hide = function() {
      modalInstance.hide();
    };

    this.dismiss = function() {
      modalInstance.dismiss();
    };

  })
  .controller('FlowStreamDetailController', function($scope, myStreamApi, $state, myFileUploader, myAlertOnValium) {

    $scope.loading = false;

    $scope.doInject = function () {
      if(!$scope.userInput) {
        $scope.userInput = null;
        return;
      }

      var params = {
        namespace: $state.params.namespace,
        streamId: $scope.streamId,
        scope: $scope
      };

      var lines = $scope.userInput.replace(/\r\n/g, '\n').split('\n');

      angular.forEach(lines, function (line) {
        myStreamApi.sendEvent(params, line);
      });

      $scope.userInput = null;
    };

    $scope.dismiss = function() {
      $scope.$dismiss();
    };

    $scope.uploadFile = function (files) {
      $scope.loading = true;
      var path = '/namespaces/' + $state.params.namespace + '/streams/' + $scope.streamId + '/batch';

      function uploadSuccess() {
        $scope.dismiss();
        $scope.loading = false;

        myAlertOnValium.show({
          type: 'success',
          title: 'Upload success',
          content: 'The file has been uploaded successfully'
        });
      }
      function uploadFailure() {
        myAlertOnValium.show({
          type: 'danger',
          title: 'Upload failed',
          content: 'The file could not be uploaded'
        });
        $scope.dismiss();
        $scope.loading = false;
      }

      for (var i = 0; i < files.length; i++) {
        // TODO: support other file types

        myFileUploader.upload({
          path: path,
          file: files[i]
        }, {
          'Content-type': 'text/csv'
        })
          .then(uploadSuccess, uploadFailure);
      }


    };
  });
