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
  .directive('myKeyValue', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '=',
        isDropdown: '='
      },
      templateUrl: 'widget-container/widget-keyvalue/widget-keyvalue.html',
      controller: function($scope, myHelpers) {
        $scope.kvdelimiter = myHelpers.objectQuery($scope.config, 'kv-delimiter') ||
                             myHelpers.objectQuery($scope.config, 'widget-attributes', 'kv-delimiter') ||
                             ':';
        $scope.delimiter = myHelpers.objectQuery($scope.config, 'delimiter') ||
                           myHelpers.objectQuery($scope.config, 'widget-attributes', 'delimiter') ||
                           ',';

        $scope.keyPlaceholder = myHelpers.objectQuery($scope.config, 'widget-attributes', 'key-placeholder') || 'key';
        $scope.valuePlaceholder = myHelpers.objectQuery($scope.config, 'widget-attributes', 'value-placeholder') || 'value';

        $scope.showDelimiter = false;
        var showDelimiterProperty = myHelpers.objectQuery($scope.config, 'widget-attributes', 'showDelimiter');
        if (($scope.config.properties && $scope.config.properties.showDelimiter === 'true') || showDelimiterProperty === 'true' ) {
          $scope.showDelimiter = true;
        }

        // Changing value field to dropdown based on config
        if ($scope.isDropdown) {
          $scope.dropdownOptions = myHelpers.objectQuery($scope.config, 'widget-attributes', 'dropdownOptions');
        }

        // initializing
        function initialize() {
          var str = $scope.model;
          $scope.properties = [];

          if (!str) {
            //intialize to one empty property
            $scope.properties.push({
              key: '',
              value: ''
            });

            return;
          }
          var arr = str.split($scope.delimiter);

          angular.forEach(arr, function(a) {
            var split = a.split($scope.kvdelimiter);

            $scope.properties.push({
              key: split[0],
              value: split[1]
            });
          });
        }

        initialize();

        $scope.$watch('properties', function() {

          var str = '';

          angular.forEach($scope.properties, function(p) {
            if (p.key.length > 0) {
              str = str + p.key + $scope.kvdelimiter + p.value + $scope.delimiter;
            }
          });

          // remove last delimiter
          if (str.length > 0 && str.charAt(str.length - 1) === $scope.delimiter ) {
            str = str.substring(0, str.length - 1);
          }

          $scope.model = str;

        }, true);


        $scope.addProperty = function() {
          $scope.properties.push({
            key: '',
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
