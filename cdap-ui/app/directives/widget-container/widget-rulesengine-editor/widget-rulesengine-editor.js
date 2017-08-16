/*
 * Copyright © 2017 Cask Data, Inc.
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
  .directive('myRulesEngineEditor', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '=',
        properties: '='
      },
      templateUrl: 'widget-container/widget-rulesengine-editor/widget-rulesengine-editor.html',
      controller: function($scope, $uibModal) {
        $scope.openRulesEngineModal = function() {
          $uibModal.open({
            controller: 'RulesEngineModalController',
            controllerAs: 'RulesEngineModalCtrl',
            windowClass: 'rules-engine-modal',
            keyboard: false,
            templateUrl: 'widget-container/widget-rulesengine-editor/rules-engine-modal.html',
            resolve: {
              rPlugin: () => {
                return {properties: $scope.properties};
              }
            }
          });
        };
      }
    };
  });
