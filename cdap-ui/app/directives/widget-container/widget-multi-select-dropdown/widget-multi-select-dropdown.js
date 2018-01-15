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
  .directive('myMultiSelectDropdown', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '='
      },
      templateUrl: 'widget-container/widget-multi-select-dropdown/widget-multi-select-dropdown.html',
      controller: function($scope, myHelpers) {
        $scope.extraSettings = {
          externalProp: '',
          checkBoxes: true
        };
        $scope.selectedOptions = [];
        $scope.delimiter = myHelpers.objectQuery($scope.config, 'widget-attributes', 'delimiter') || ',';
        $scope.options = myHelpers.objectQuery($scope.config, 'widget-attributes', 'options') || [];
        $scope.options = $scope.options.map(option => {
          return {
            id: option.id,
            label: option.label || option.id
          };
        });
        let defaultValue = myHelpers.objectQuery($scope.config, 'widget-attributes', 'defaultValue') || [];
        if ($scope.model) {
          $scope.model
            .split($scope.delimiter)
            .forEach(value => {
              let valueInOption = $scope.options.find(op => op.id === value);
              if (valueInOption) {
                $scope.selectedOptions.push(valueInOption);
              } else {
                let unknownValue = {
                  id: value,
                  label: 'UnKnown Value (' + value + '). Not part of options'
                };
                $scope.options.push(unknownValue);
                $scope.selectedOptions.push(unknownValue);
              }
            });
        } else {
          let defaultOption;
          defaultOption = defaultValue
            .map(value => $scope.options.find(op => op.id === value))
            .filter(value => value);

          if (defaultOption.length) {
            $scope.selectedOptions = $scope.selectedOptions.concat(defaultOption);
          }
        }
        $scope.$watch('selectedOptions', function() {
          $scope.model = $scope.selectedOptions.map(o => o.id). join($scope.delimiter);
        }, true);
      }
    };
  });
