/*
 * Copyright © 2016 Cask Data, Inc.
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
  .directive('myDsMultipleValues', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '='
      },
      templateUrl: 'widget-container/widget-ds-multiplevalues/widget-ds-multiplevalues.html',
      controller: function($scope, myHelpers) {
        $scope.valuesdelimiter = myHelpers
          .objectQuery($scope.config, 'widget-attributes', 'values-delimiter') || ':';
        $scope.delimiter = myHelpers
          .objectQuery($scope.config, 'widget-attributes', 'delimiter') || ',';
        $scope.numValues = myHelpers
          .objectQuery($scope.config, 'widget-attributes', 'numValues') || 2;
        $scope.placeholders = myHelpers
          .objectQuery($scope.config, 'widget-attributes', 'placeholders') || [];
        $scope.numValues = parseInt($scope.numValues, 10);
        $scope.showDelimiter = false;
        var showDelimiterProperty = myHelpers.objectQuery($scope.config, 'widget-attributes', 'showDelimiter');
        if (( $scope.config && $scope.config.properties && $scope.config.properties.showDelimiter === 'true') || showDelimiterProperty === 'true' ) {
          $scope.showDelimiter = true;
        }


        // initializing
        function initialize() {
          var str = $scope.model;
          $scope.properties = [];

          if (!str) {
            //intialize to one empty property
            $scope.properties.push({
              values: Array($scope.numValues).join('.').split('.')
            });

            return;
          }
          var arr = str.split($scope.delimiter);

          angular.forEach(arr, function(a) {
            var split = a.split($scope.valuesdelimiter);
            $scope.properties.push({
              values: split
            });
          });
        }

        initialize();

        $scope.$watch('properties', function() {
          var str = '';

          angular.forEach($scope.properties, function(p) {
            var isAnyEmptyValue = p.values.filter(function(val) { return val.length === 0; });
            if (isAnyEmptyValue.length) { return; }
            str = str + p.values.join($scope.valuesdelimiter) + $scope.delimiter;
          });

          // remove last delimiter
          if (str.length > 0 && str.charAt(str.length - 1) === $scope.delimiter ) {
            str = str.substring(0, str.length - 1);
          }

          $scope.model = str;

        }, true);


        $scope.addProperty = function() {
          $scope.properties.push({
            values: Array($scope.numValues).join('.').split('.'),
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
