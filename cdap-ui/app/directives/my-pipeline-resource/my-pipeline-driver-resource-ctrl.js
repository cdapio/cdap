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
function MyPipelineDriverResourceCtrl($scope, HYDRATOR_DEFAULT_VALUES) {
  'ngInject';
  $scope.virtualCores = $scope.virtualCoresValue || $scope.store.getDriverVirtualCores();
  $scope.memoryMB = $scope.memoryMbValue || $scope.store.getDriverMemoryMB();
  $scope.cores = Array.apply(null, {length: 20}).map((ele, index) => index+1);
  $scope.numberConfig = {
    'widget-attributes': {
      min: 0,
      default: HYDRATOR_DEFAULT_VALUES.resources.memoryMB,
      showErrorMessage: false,
      convertToInteger: true
    }
  };
  $scope.$watch('memoryMB', function(oldValue, newValue) {
    if (oldValue === newValue) {
      return;
    }
    if ($scope.onMemoryChange && typeof $scope.onMemoryChange === 'function') {
      var fn = $scope.onMemoryChange();
      if ('undefined' !== typeof fn) {
        fn.call()($scope.memoryMB);
      }
    } else {
      if (!$scope.isDisabled){
        $scope.actionCreator.setDriverMemoryMB($scope.memoryMB);
      }
    }
  });
  $scope.onVirtualCoresChange = function() {
    if ($scope.onCoreChange && typeof $scope.onCoreChange === 'function') {
      var fn = $scope.onCoreChange();
      if ('undefined' !== typeof fn) {
        fn.call()($scope.virtualCores);
      }
    } else {
      if (!$scope.isDisabled){
        $scope.actionCreator.setDriverVirtualCores($scope.virtualCores); 
      }
    }
  };
}
angular.module(PKG.name + '.commons')
  .controller('MyPipelineDriverResourceCtrl', MyPipelineDriverResourceCtrl);
