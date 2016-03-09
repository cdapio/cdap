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
  .directive('myNumberWidget', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '='
      },
      template: '<input type="number" class="form-control" min="{{min}}" max="{{max}}" ng-model="internalModel" />',
      controller: function($scope, myHelpers) {
        $scope.model = $scope.model ||
                       myHelpers.objectQuery($scope.config, 'properties', 'default') ||
                       myHelpers.objectQuery($scope.config, 'widget-attributes', 'default');
        $scope.internalModel = $scope.model;
        // The number textbox requires the input to be number.
        // This will be correct for a fresh create studio view. But when the user is trying to import or clone
        // it would be a problem as the imported/cloned plugin property would be a string and number textbox
        // expects a number. Hence this internal state. 'internalModel' is for maintaining the model as number
        // for number textbox and the model is actual model being saved as property value.
        if (typeof $scope.model !== 'number') {
          $scope.internalModel = parseInt($scope.model, 10);
        }
        $scope.$watch('internalModel', function(newValue, oldValue) {
          if (oldValue === newValue) {
            return;
          }
          $scope.model = $scope.internalModel && $scope.internalModel.toString();
        });

        // This is needed when we hit reset in node configuration.
        $scope.$watch('model', function() {
          $scope.internalModel = parseInt($scope.model, 10);
        });
        $scope.min = $scope.config.min || '';
        $scope.max = $scope.config.max || '';
      }
    };
  });
