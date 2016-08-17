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

class FlowsRunDetailLogController {
  constructor($scope, $state) {

    this.params = {
      namespaceId: $state.params.namespace,
      appId: $state.params.appId,
      programType: 'flows',
      programId: $state.params.programId,
      runId: $scope.RunsController.runs.length ? $scope.RunsController.runs.selected.runid : '',
    };
  }
}
FlowsRunDetailLogController.$inject = ['$scope', '$state'];
angular.module(`${PKG.name}.feature.flows`)
  .controller('FlowsRunDetailLogController', FlowsRunDetailLogController);
