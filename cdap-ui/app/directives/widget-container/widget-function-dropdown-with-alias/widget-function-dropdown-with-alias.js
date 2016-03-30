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
   .directive('myFunctionDropdownWithAlias', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '=',
      },
      templateUrl: 'widget-container/widget-function-dropdown-with-alias/widget-function-dropdown-with-alias.html',
      controller: function($scope, myHelpers) {
        $scope.functionAliasList = [];
        $scope.placeholders = myHelpers.objectQuery($scope.config, 'widget-attributes', 'placeholders');
        $scope.dropdownOptions = myHelpers.objectQuery($scope.config, 'widget-attributes', 'dropdownOptions');
        if (angular.isObject($scope.placeholders)) {
          $scope.aliasPlaceholder = $scope.placeholders.alias || '';
          $scope.fieldPlaceholder = $scope.placeholders.field || '';
        }

        $scope.addFunctionAlias = function() {
          $scope.functionAliasList.push({
            functionName: '',
            field:'',
            alias: ''
          });
        };
        $scope.removeFunctionAlias = function(index) {
          $scope.functionAliasList.splice(index, 1);
          if (!$scope.functionAliasList.length) {
            $scope.addFunctionAlias();
          }
        };
        $scope.convertInternalToExternalModel = function() {
          var externalModel = '';
          $scope.functionAliasList.forEach( function(fnAlias) {
            if ([fnAlias.functionName.length, fnAlias.field.length, fnAlias.alias.length].indexOf(0) !== -1) {
              return;
            }
            if (externalModel.length) {
              externalModel += ',';
            }
            externalModel += fnAlias.alias + ':' + fnAlias.functionName + '(' + fnAlias.field + ')';
          });
          return externalModel;
        };
        $scope.convertExternalToInternalModel = function() {
          var functionsAliasList = $scope.model.split(',');
          functionsAliasList.forEach( function (fnAlias) {
            if (!fnAlias.length) {
              return;
            }
            var aliasEndIndex = fnAlias.indexOf(':');
            var fnEndIndex = fnAlias.indexOf('(');
            var fieldEndIndex = fnAlias.indexOf(')');
            if ([aliasEndIndex, fnEndIndex, fieldEndIndex].indexOf(-1) !== -1) {
              return;
            }
            $scope.functionAliasList.push({
              alias: fnAlias.substring(0, aliasEndIndex),
              functionName: fnAlias.substring(aliasEndIndex+1, fnEndIndex),
              field: fnAlias.substring(fnEndIndex+1, fieldEndIndex)
            });
          });
        };

        if ($scope.model && $scope.model.length) {
          $scope.convertExternalToInternalModel();
        } else {
          $scope.addFunctionAlias();
        }
        $scope.$watch('functionAliasList', function() {
          $scope.model = $scope.convertInternalToExternalModel();
        }, true);
      }
    };
  });
