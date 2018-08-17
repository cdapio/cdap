/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

angular.module(PKG.name+'.commons')
  .directive('caskDropdownTextCombo', function caskDropdownTextComboDirective() {
    return {
      restrict: 'E',
      scope: {
        model: '=',
        dropdownList: '=',
        textFields: '=',
        assetLabel: '@'
      },
      templateUrl: 'cask-angular-dropdown-text-combo/dropdown-text-combo.html',
      link: function ($scope) {
        $scope.dropdownValues = [];

        function buildDropdown () {
          //dropdownList doesn't always needs to be a $resource object with a promise.
          if(
              (
                $scope.dropdownList.$promise &&
                  !$scope.dropdownList.$resolved
              )||
              !$scope.model) {
            return;
          }
          $scope.dropdownValues = $scope.dropdownList
            .filter(function (item) {
              var isValid = Object.keys($scope.model)
                                  .indexOf(item.name) === -1;
              return isValid;
            })
            .map(function (item) {
              return {
                text: item.name,
                click: 'addAsset(\"'+item.name+'\")'
              };
            });
        }

        //dropdownList doesn't always needs to be a $resource object with a promise.
        if ($scope.dropdownList.$promise) {
          $scope.dropdownList.$promise.then(buildDropdown);
        }

        $scope.$watchCollection('model', buildDropdown);

        $scope.rmAsset = function (pName) {
          delete $scope.model[pName];
        };

        $scope.addAsset = function (pName) {
          if(!$scope.model) { return; }

          $scope.model[pName] = {
            name: pName
          };

        };
      }
    };
  });
