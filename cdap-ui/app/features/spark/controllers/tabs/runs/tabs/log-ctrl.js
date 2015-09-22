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

angular.module(PKG.name + '.feature.spark')
  .controller('SparkRunDetailLogController', function($scope, $state, mySparkApi, $timeout) {

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      sparkId: $state.params.programId,
      runId: $scope.RunsController.runs.selected.runid,
      max: 50,
      scope: $scope
    };

    if (!$scope.RunsController.runs.length) {
      return;
    }

    this.loadingNext = true;
    mySparkApi.prevLogs(params)
      .$promise
      .then(function (res) {
        this.logs = res;
        this.loadingNext = false;
      }.bind(this));

    this.loadNextLogs = function () {
      if (this.loadingNext) {
        return;
      }

      this.loading = true;
      params.fromOffset = this.logs[this.logs.length-1].offset;

      mySparkApi.nextLogs(params)
        .$promise
        .then(function (res) {
          this.logs = _.uniq(this.logs.concat(res));
          this.loadingNext = false;
        }.bind(this));
    };

    this.loadPrevLogs = function () {
      if (this.loadingPrev) {
        return;
      }

      this.loadingPrev = true;
      params.fromOffset = this.logs[0].offset;

      mySparkApi.prevLogs(params)
        .$promise
        .then(function (res) {
          this.logs = _.uniq(res.concat(this.logs));
          this.loadingPrev = false;

          $timeout(function() {
            var container = angular.element(document.querySelector('[infinite-scroll]'))[0];
            var logItem = angular.element(document.getElementById(params.fromOffset))[0];
            container.scrollTop = logItem.offsetTop;
          });
        }.bind(this));
    };
});
