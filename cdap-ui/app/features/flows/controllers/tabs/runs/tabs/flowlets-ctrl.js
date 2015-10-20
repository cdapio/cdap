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

class FlowletsController {
  constructor($scope, $state, $filter, FlowDiagramData) {
    this.flowlets = [];
    this.$filter = $filter;
    let filterFilter = this.$filter('filter');
    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then( (res) => {
        let match;
        angular.forEach(res.flowlets, (v) => {
          let name = v.flowletSpec.name;
          v.isOpen = false;
          this.flowlets.push({name: name, isOpen: $state.params.flowletid === name});
        });

        if (!$scope.RunsController.activeFlowlet) {
          this.flowlets[0].isOpen = true;
          this.activeFlowlet = this.flowlets[0];
        } else {
          match = filterFilter(this.flowlets, {name: $scope.RunsController.activeFlowlet});
          match[0].isOpen = true;
          this.activeFlowlet = match[0];
        }

      });
  }
  selectFlowlet(flowlet) {
    let filterFilter = this.$filter('filter');
    angular.forEach(this.flowlets, function(f) {
      f.isOpen = false;
    });
    let match = filterFilter(this.flowlets, flowlet);
    match[0].isOpen = true;
    this.activeFlowlet = match[0];
  }
}

FlowletsController.$inject = ['$scope', '$state', '$filter', 'FlowDiagramData'];
angular.module(PKG.name + '.feature.flows')
  .controller('FlowletsController', FlowletsController);
