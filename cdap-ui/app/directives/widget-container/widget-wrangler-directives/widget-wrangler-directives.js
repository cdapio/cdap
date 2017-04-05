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
  .directive('myWranglerDirectives', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '=',
        properties: '='
      },
      templateUrl: 'widget-container/widget-wrangler-directives/widget-wrangler-directives.html',
      controller: function($scope, myHelpers, $stateParams, myDataprepApi, EventPipe, $uibModal) {
        $scope.rows = myHelpers.objectQuery($scope, 'config', 'widget-attributes', 'rows');
        $scope.placeholder = myHelpers.objectQuery($scope, 'config', 'widget-attributes', 'rows') || '';
        $scope.fetchWorkspaceDetails = function() {
          if ($scope.properties.workspaceId) {
            let workspaceId =  $scope.properties.workspaceId;
            let requestObj = {
              namespace: $stateParams.namespace,
              workspaceId: $scope.properties.workspaceId
            };
            myDataprepApi.getWorkspace(requestObj)
              .$promise
              .then(res => {
                let directives = myHelpers.objectQuery(res, 'values', 0, 'recipe', 'directives');
                $scope.model = directives.join('\n');
                let requestBody = window.CaskCommon.DataPrepHelper.directiveRequestBodyCreator(directives, workspaceId);

                myDataprepApi
                  .getSchema(requestObj, requestBody)
                  .$promise
                  .then(fields => {
                    let schemaValue = {
                      name: 'avroSchema',
                      type: 'record',
                      fields
                    };
                    EventPipe.emit('schema.import', JSON.stringify(schemaValue));
                  });
              });
          }
        };
        EventPipe.on('wrangler.updateworkspace', $scope.fetchWorkspaceDetails);
        $scope.fetchWorkspaceDetails();

        $scope.openWranglerModal = function() {
          $uibModal.open({
            controller: 'WranglerModalController',
            controllerAs: 'Wrangler',
            windowClass: 'wrangler-modal',
            keyboard: false,
            templateUrl: '/assets/features/hydrator/templates/create/Wrangler/wrangler-modal.html',
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
