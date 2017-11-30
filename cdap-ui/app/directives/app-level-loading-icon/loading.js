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

angular
  .module(PKG.name + '.commons')
  .directive('loadingIcon', function(
    myLoadingService,
    $uibModal,
    $timeout,
    EventPipe,
    $state,
    myAlertOnValium
  ) {
    return {
      restrict: 'EA',
      scope: true,
      template: '<div></div>',
      controller: function($scope) {
        var modalObj = {
            templateUrl: 'app-level-loading-icon/loading.html',
            backdrop: 'static',
            keyboard: true,
            scope: $scope,
            windowClass: 'custom-loading-modal'
          },
          modal;
        var genericServiceErrorMsg = 'CDAP Services are not available';
        var genericSubtitle = 'Trying to connect...';
        var hideLoadingTimeout = null;
        $scope.adminUrl = '';

        EventPipe.on(
          'backendDown',
          function(message, subtitle) {
            if (!$scope.backendDown) {
              if (modal) {
                modal.close();
              }
              $scope.message = message || genericServiceErrorMsg;
              $scope.subtitle = subtitle || genericSubtitle;
              $scope.backendDown = true;
              $scope.adminUrl = window.location.protocol + '//' + window.location.host + '/cdap/administration';
              modal = $uibModal.open(modalObj);
              modal.result.finally(function() {
                $state.go('overview', { reload: true });
              });
            } else {
              $scope.message = message || genericServiceErrorMsg;
              $scope.subtitle = subtitle || genericSubtitle;
            }
          }.bind($scope)
        );

        EventPipe.on(
          'backendUp',
          function(message) {
            if ($scope.backendDown) {
              modal.close();
              modal = null;
              $scope.backendDown = false;

              myAlertOnValium.show({
                type: 'success',
                content: message ? message : 'Services are online'
              });
            }
          }.bind($scope)
        );

        EventPipe.on('hideLoadingIcon', function() {
          // Just making it smooth instead of being too 'speedy'

          $timeout.cancel(hideLoadingTimeout);
          hideLoadingTimeout = $timeout(function() {
            if (!$scope.backendDown) {
              if (modal && !modal.$state) {
                modal.close();
              }
              modal = null;
            }
          }, 2000);
        });

        // Should use this hide when we are just loading a state
        EventPipe.on('hideLoadingIcon.immediate', function() {
          if (modal) {
            // This is needed if the loading icon is shown and closed even before opened.
            // EventPipe will execute the listener immediately when the event is emitted,
            // however $alert which internally used $modal opens up only during next tick.
            // If the modal is opened and is closed at some point later (normal usecase),
            // the 'opened' promise is still resolved and the alert is closed.
            modal.opened.then(function() {
              modal.close();
              modal = null;
            });
          }
        });

        EventPipe.on(
          'showLoadingIcon',
          function(message) {
            if (!modal && !$scope.backendDown) {
              $scope.message = message || '';
              modal = $uibModal.open(modalObj);
            }
          }.bind($scope)
        );
      }
    };
  });
