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

angular.module(PKG.name + '.commons')
  .directive('myJsonTextbox', function() {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        placeholder: '='
      },
      template: '<textarea class="form-control" data-ng-trim="false" cask-json-edit="internalModel" placeholder="placeholder"></textarea>',
      controller: function($scope) {
        function initialize () {
          try {
            $scope.internalModel = JSON.parse($scope.model);
          } catch(e) {
            $scope.internalModel = '';
          }
        }

        initialize();

        $scope.$watch('model', initialize);

        $scope.$watch('internalModel', function(newVal, oldVal) {
          if (newVal !== oldVal) {
            $scope.model = angular.toJson($scope.internalModel);
          }
        });
      }
    };
  });
