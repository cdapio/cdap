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

function JumpController ($scope, myJumpFactory) {
  'ngInject';

  let vm = this;

  if ($scope.entityType === 'datasets') {
    vm.isAvailableDataset = myJumpFactory.isAvailableDataset($scope.datasetType);
  }

  vm.streamBatchSource = () => {
    myJumpFactory.streamBatchSource($scope.entityId);
  };
  vm.streamRealtimeSink = () => {
    myJumpFactory.streamRealtimeSink($scope.entityId);
  };

  vm.datasetBatchSource = () => {
    myJumpFactory.datasetBatchSource($scope.entityId);
  };
  vm.datasetBatchSink = () => {
    myJumpFactory.datasetBatchSink($scope.entityId);
  };

}

angular.module(PKG.name + '.feature.tracker')
  .directive('myJumpButton', () => {
    return {
      restrict: 'E',
      scope: {
        entityType: '=',
        entityId: '=',
        datasetType: '=?',
        datasetExplorable: '='
      },
      templateUrl: '/old_assets/features/tracker/directives/jump/jump.html',
      controller: JumpController,
      controllerAs: 'Jump'
    };
  });
