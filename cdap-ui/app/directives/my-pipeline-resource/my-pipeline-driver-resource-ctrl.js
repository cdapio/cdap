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
function MyPipelineDriverResourceCtrl($scope) {
  'ngInject';
  $scope.virtualCores = $scope.store.getDriverVirtualCores();
  $scope.memoryMB = $scope.store.getDriverMemoryMB();
  $scope.cores = Array.apply(null, {length: 21}).map(Number.call, Number).filter(a => a > 0);

  $scope.onMemoryMBChange = function() {
    $scope.actionCreator.setDriverMemoryMB($scope.memoryMB);
  };
  $scope.onVirtualCoresChange = function() {
    $scope.actionCreator.setDriverVirtualCores($scope.virtualCores);
  };
}
angular.module(PKG.name + '.commons')
  .controller('MyPipelineDriverResourceCtrl', MyPipelineDriverResourceCtrl);
