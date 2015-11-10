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
  .directive('myDsv', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        delimiter: '@',
        type: '@'
      },
      templateUrl: 'widget-container/widget-dsv/widget-dsv.html',
      controller: function($scope, EventPipe) {
        var modelCopy = angular.copy($scope.model);

        if ($scope.type === 'csv') {
          $scope.showDelimiter = false;
        } else {
          $scope.showDelimiter = true;
        }

        var delimiter = $scope.delimiter || ',';

        // initializing
        function initialize() {
          var str = $scope.model;
          $scope.properties = [];

          if (!str) {
            // initialize with empty value
            $scope.properties.push({
              value: ''
            });
            return;
          }
          var arr = str.split(delimiter);

          angular.forEach(arr, function(a) {
            $scope.properties.push({
              value: a
            });
          });

        }

        initialize();

        EventPipe.on('plugin.reset', function () {
          $scope.model = angular.copy(modelCopy);

          initialize();
        });

        var propertyListener = $scope.$watch('properties', function() {
          var str = '';

          angular.forEach($scope.properties, function(p) {
            if (p.value) {
              str = str + p.value + delimiter;
            }
          });

          // remove last delimiter
          if (str.length > 0 && str.charAt(str.length - 1) === delimiter ) {
            str = str.substring(0, str.length - 1);
          }

          $scope.model = str;

        }, true);

        $scope.$on('$destroy', function() {
          propertyListener();
        });

        $scope.addProperty = function() {
          $scope.properties.push({
            value: '',
            newField: 'add'
          });
        };

        $scope.removeProperty = function(property) {
          var index = $scope.properties.indexOf(property);
          $scope.properties.splice(index, 1);
        };

        $scope.enter = function (event, last) {
          if (last && event.keyCode === 13) {
            $scope.addProperty();
          }
        };

      }
    };
  });
