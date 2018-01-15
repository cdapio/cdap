/*
 * Copyright © 2018 Cask Data, Inc.
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
  .directive('myRadioGroup', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '='
      },
      templateUrl: 'widget-container/widget-radio-group/widget-radio-group.html',
      controller: function(myHelpers, $scope, uuid) {
        $scope.groupName = 'radio-group-'+ uuid.v4();
        $scope.options = myHelpers.objectQuery($scope.config, 'widget-attributes', 'options') || [];
        if (!Array.isArray($scope.options) || (Array.isArray($scope.options) && !$scope.options.length)) {
          $scope.error = 'Missing options for ' + myHelpers.objectQuery($scope.config, 'name');
        }
        let defaultValue = myHelpers.objectQuery($scope.config, 'widget-attributes', 'default') || '';
        $scope.layout = myHelpers.objectQuery($scope.config, 'widget-attributes', 'layout') || 'block';
        $scope.model = $scope.model || defaultValue;
        let isModelValid = $scope.options.find(option => option.id === $scope.model);
        if (!isModelValid) {
          $scope.error = 'Unknown value for ' + myHelpers.objectQuery($scope.config, 'name') + ' specified.';
        }
        $scope.$watch('model', () => {
          let isModelValid = $scope.options.find(option => option.id === $scope.model);
          if (isModelValid) {
            $scope.error = null;
          }
        });
        $scope.options = $scope.options.map(option => ({
          id: option.id,
          label: option.label || option.id
        }));
      }
    };
  });
