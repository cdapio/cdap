/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

angular.module(PKG.name + '.commons')
  .directive('loadingIcon', function(myLoadingService, $uibModal, $timeout, EventPipe) {
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
        }, modal, isBackendDown = false;
        var hideLoadingTimeout = null;

        EventPipe.on('hideLoadingIcon', function() {
          // Just making it smooth instead of being too 'speedy'

          $timeout.cancel(hideLoadingTimeout);
          hideLoadingTimeout = $timeout(function() {
            if (!isBackendDown) {
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

        EventPipe.on('showLoadingIcon', function(message, userCloseEnabled) {
          if (!modal && !isBackendDown) {
            $scope.message = message || 'Loading...';
            if (!userCloseEnabled) {
              modalObj.keyboard = false;
            } else {
              modalObj.keyboard = true;
            }
            modal = $uibModal.open(modalObj);
          }
        }.bind($scope));
      }
    };
  });
