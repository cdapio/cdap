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
        disabled: '=',
        model: '=ngModel',
        config: '=',
        isFieldRequired: '='
      },
      templateUrl: 'widget-container/widget-number/widget-number.html',
      controller: function($scope, myHelpers) {
        var defaultValue = myHelpers.objectQuery($scope.config, 'widget-attributes', 'default');
        $scope.model = $scope.model || angular.copy(defaultValue);
        $scope.internalModel = $scope.model;
        var minValueFromWidgetJSON = myHelpers.objectQuery($scope.config, 'widget-attributes', 'min');
        var maxValueFromWidgetJSON = myHelpers.objectQuery($scope.config, 'widget-attributes', 'max');
        $scope.showErrorMessage = myHelpers.objectQuery($scope.config, 'widget-attributes', 'showErrorMessage');
        $scope.convertToInteger = myHelpers.objectQuery($scope.config, 'widget-attributes', 'convertToInteger') || false;
        // The default is to show the message i.e., true
        // We need to explicitly pass a false to hide it.
        // Usually we don't pass this attribute and in that case undefined (or no value) means show the message. Hence the comparison.
        $scope.showErrorMessage = $scope.showErrorMessage === false ? false: true;
        if (typeof minValueFromWidgetJSON === 'number') {
          minValueFromWidgetJSON = minValueFromWidgetJSON.toString();
        }
        if (typeof maxValueFromWidgetJSON === 'number') {
          maxValueFromWidgetJSON = maxValueFromWidgetJSON.toString();
        }
        var checkForBounds = function(newValue) {
          if (!$scope.isFieldRequired && !newValue) {
            $scope.error = '';
            return true;
          }
          if ($scope.disabled) {
            return true;
          }
          if (!newValue) {
            $scope.error = 'Value cannot be empty';
            if (defaultValue) {
              $scope.error += '. Default value: ' + defaultValue;
            }
            return false;
          }
          if (newValue < $scope.min || newValue > $scope.max) {
            $scope.error = 'Value exceeds the limit [min: ' + $scope.min + ', max: ' + $scope.max + ']';
            return false;
          }
          $scope.error = '';
        };

        $scope.min =  minValueFromWidgetJSON || -Infinity;
        $scope.max =  maxValueFromWidgetJSON || Infinity;
        // The number textbox requires the input to be number.
        // This will be correct for a fresh create studio view. But when the user is trying to import or clone
        // it would be a problem as the imported/cloned plugin property would be a string and number textbox
        // expects a number. Hence this internal state. 'internalModel' is for maintaining the model as number
        // for number textbox and the model is actual model being saved as property value.
        if (typeof $scope.model !== 'number') {
          $scope.internalModel = parseInt($scope.model, 10);
        }
        $scope.$watch('internalModel', function(newValue, oldValue) {
          if (oldValue === newValue || !checkForBounds(newValue)) {
            $scope.model = (typeof newValue === 'number' && !Number.isNaN(newValue) && newValue) || '';
            if (!$scope.convertToInteger) {
              $scope.model = $scope.model.toString();
            }
            return;
          }
          $scope.model = (typeof $scope.internalModel === 'number' && !Number.isNaN($scope.internalModel) && $scope.internalModel) || '';
          if (!$scope.convertToInteger && $scope.internalModel) {
            $scope.model = $scope.internalModel.toString();
          }
        });

        // This is needed when we hit reset in node configuration.
        $scope.$watch('model', function() {
          $scope.internalModel = parseInt($scope.model, 10);
          checkForBounds($scope.internalModel);
        });
      }
    };
  });
