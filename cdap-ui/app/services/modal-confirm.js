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

angular.module(PKG.name+'.services')
  .factory('ModalConfirm', function($bootstrapModal) {

    // TODO: need to make this factory more modular

    function confirmModalAdapter (scope, plugin, copy, closeCb) {
      var watch = scope.$on('modal.closing', function (event, reason) {
        if ((reason === 'cancel' || reason === 'escape key press') && !scope.confirm ) {
          var stringCopy = JSON.stringify(copy);
          var stringPlugin = JSON.stringify(plugin);

          if (stringCopy !== stringPlugin) {
            event.preventDefault();

            var confirmInstance = $bootstrapModal.open({
              keyboard: false,
              templateUrl: '/assets/features/adapters/templates/partial/confirm.html',
              windowClass: 'modal-confirm',
              controller: ['$scope', function ($scope) {
                $scope.continue = function () {
                  $scope.$close('close');
                };

                $scope.cancel = function () {
                  $scope.$close('keep open');
                };
              }]
            });

            confirmInstance.result.then(function (closing) {
              if (closing === 'close') {
                scope.confirm = true;
                closeCb();
              }
            });
          }
        }
      });

      scope.$on('$destroy', function () {
        watch();
      });
    }

    return {
      confirmModalAdapter: confirmModalAdapter
    };

  });
