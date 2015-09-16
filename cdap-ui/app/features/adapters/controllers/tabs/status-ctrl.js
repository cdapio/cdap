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

angular.module(PKG.name + '.feature.adapters')
  .controller('AdaptersDetailStatusController', function($scope, AdapterDetail) {
    var params = {};
    angular.copy(AdapterDetail.params, params);
    params.scope = $scope;

    $scope.stats = {};

    $scope.programType = AdapterDetail.programType;

    AdapterDetail.api.runs(params)
      .$promise
      .then(function (runs) {
        $scope.stats.numRuns = runs.length;
        $scope.stats.lastRunTime = runs.length > 0 && runs[0].end ? runs[0].end - runs[0].start : 'N/A';
      });

    if (AdapterDetail.programType === 'WORKFLOWS') {
      AdapterDetail.api.getStatistics(params)
        .$promise
        .then(function (stats) {
          $scope.stats.avgRunTime = stats.avgRunTime ? stats.avgRunTime : 'N/A';
        });
    }
  });
