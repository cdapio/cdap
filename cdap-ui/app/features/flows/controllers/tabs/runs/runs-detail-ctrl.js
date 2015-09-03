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

angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailController', function($scope, $state, $filter) {
    var filterFilter = $filter('filter'),
        match;
    match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    // If there is no match then there is something wrong with the runid in the URL.
    $scope.RunsController.runs.selected.runid = match[0].runid;
    $scope.RunsController.runs.selected.status = match[0].status;
    $scope.RunsController.runs.selected.start = match[0].start;
    $scope.RunsController.runs.selected.end = match[0].end;

    $scope.$on('$destroy', function() {
      $scope.RunsController.runs.selected.runid = null;
    });


  });
