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

class FlowsRunDetailController {
  constructor($scope, $state, $filter) {
    this.$scope = $scope;
    this.$state = $state;
    this.$filter = $filter;

    let filterFilter = this.$filter('filter'),
        match;
    match = filterFilter(this.$scope.RunsController.runs, {runid: this.$state.params.runid});
    // If there is no match then there is something wrong with the runid in the URL.
    this.$scope.RunsController.runs.selected.runid = match[0].runid;
    this.$scope.RunsController.runs.selected.status = match[0].status;
    this.$scope.RunsController.runs.selected.start = match[0].start;
    this.$scope.RunsController.runs.selected.end = match[0].end;

    this.$scope.$on('$destroy', () => {
      this.$scope.RunsController.runs.selected.runid = null;
    });

  }
}
FlowsRunDetailController.$inject = ['$scope', '$state', '$filter'];

angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailController', FlowsRunDetailController);
