/*
 * Copyright © 2020 Cask Data, Inc.
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
.directive('myFunctionDropdownWithOptions', function() {
 return {
   restrict: 'E',
   scope: {
     model: '=ngModel',
     config: '=',
   },
   templateUrl: 'widget-container/widget-function-dropdown-with-options/widget-function-dropdown-with-options.html',
   controller: function($scope, myHelpers) {
     $scope.functionOptionList = [];
     $scope.placeholders = myHelpers.objectQuery($scope.config, 'widget-attributes', 'placeholders');
     $scope.dropdownOptions = myHelpers.objectQuery($scope.config, 'widget-attributes', 'dropdownOptions');
     if (angular.isObject($scope.placeholders)) {
       $scope.aliasPlaceholder = $scope.placeholders.alias || '';
       $scope.fieldPlaceholder = $scope.placeholders.field || '';
     }

     $scope.addFunctionOption = function() {
       $scope.functionOptionList.push({
         functionName: '',
         field:'',
         alias: '',
         arguments: '',
         ignoreNulls: true
       });
     };
     $scope.removeFunctionOption = function(index) {
       $scope.functionOptionList.splice(index, 1);
       if (!$scope.functionOptionList.length) {
         $scope.addFunctionOption();
       }
     };
     $scope.convertInternalToExternalModel = function() {
       var externalModel = '';
       $scope.functionOptionList.forEach( function(fnOption) {
         if ([fnOption.functionName.length, fnOption.field.length, fnOption.alias.length].indexOf(0) !== -1) {
           return;
         }
         if (externalModel.length) {
           externalModel += ',';
         }
         externalModel += fnOption.alias + ':' + fnOption.functionName + '(' + fnOption.field + ')';
         // TODO Add arguments and ignoreNulls
       });
       return externalModel;
     };
     $scope.convertExternalToInternalModel = function() {
       var functionsAliasList = $scope.model.split(',');
       functionsAliasList.forEach( function (fnOption) {
         if (!fnOption.length) {
           return;
         }
         var aliasEndIndex = fnOption.indexOf(':');
         var fnEndIndex = fnOption.indexOf('(');
         var fieldEndIndex = fnOption.indexOf(')');
         if ([aliasEndIndex, fnEndIndex, fieldEndIndex].indexOf(-1) !== -1) {
           return;
         }
         $scope.functionOptionList.push({
           alias: fnOption.substring(0, aliasEndIndex),
           functionName: fnOption.substring(aliasEndIndex+1, fnEndIndex),
           field: fnOption.substring(fnEndIndex+1, fieldEndIndex),
           arguments: '',
           ignoreNulls: true
           // TODO Parse arguments and ignoreNulls
         });
       });
     };

     if ($scope.model && $scope.model.length) {
       $scope.convertExternalToInternalModel();
     } else {
       $scope.addFunctionOption();
     }
     $scope.$watch('functionOptionList', function() {
       $scope.model = $scope.convertInternalToExternalModel();
     }, true);
   }
 };
});
