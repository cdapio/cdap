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

    AdapterDetail.api.pollRuns(params)
      .$promise
      .then(function (runs) {
        $scope.stats.numRuns = runs.length;
        $scope.stats.lastRunTime = runs.length > 0 && runs[0].end ? runs[0].end - runs[0].start : 'N/A';

        for (var i = 0; i < runs.length; i++) {
          var status = runs[i].status;

          if (['RUNNING', 'STARTING', 'STOPPING'].indexOf(status) === -1) {
            $scope.stats.lastFinished = runs[i];
            break;
          }
        }
      });

    if (AdapterDetail.programType === 'WORKFLOWS') {
      AdapterDetail.api.pollStatistics(params)
        .$promise
        .then(function (stats) {
          $scope.stats.avgRunTime = stats.avgRunTime ? stats.avgRunTime : 'N/A';
        });
    }
  });
