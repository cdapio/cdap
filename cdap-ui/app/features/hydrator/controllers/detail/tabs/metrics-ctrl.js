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

angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorDetailMetricsController', function(DetailRunsStore, MetricsStore, PipelineDetailMetricsActionFactory, $scope) {

    var currentRunId;
    this.setState = function() {
      this.state = {
        metrics: MetricsStore.getMetrics()
      };
    };

    this.setState();

    MetricsStore.registerOnChangeListener(this.setState.bind(this));
    DetailRunsStore.registerOnChangeListener(checkAndPollForMetrics.bind(this));

    function checkAndPollForMetrics() {
      var latestRun;
      if (DetailRunsStore.getRunsCount()) {
        latestRun = DetailRunsStore.getLatestRun();
        if (latestRun && latestRun.status !== 'RUNNING') {
          currentRunId = null;
          /*
            TL;DR - This is to avoid the in-consistent value that appears in UI due to delay in stopping a poll.

            Bigger version -
            Say we started a poll and we are actively polling for metrics for a runid (poll interval is 10sec default).

            t=1 -> made a new request for metrics
            t=2,3,4,5,6,7,8 -> Do nothing
            t=9 -> Stop the poll for metrics (meaning stop the run).

            In the past 8 seconds the metrics could have changed (we don't know for sure).
            So at the moment for a killed run we show a metric that is 8 seconds old.
            But when the user refreshes the page he/she would see the latest metric value before it is stopped.

            To avoid this in-consistency we make a final request before stopping the poll for a runid.
          */
          getMetricsForLatestRunId(false);
          PipelineDetailMetricsActionFactory.stopMetricsPoll();
          PipelineDetailMetricsActionFactory.stopMetricValuesPoll();
        } else {
          getMetricsForLatestRunId(true);
          this.setState();
        }
      }
    }

    // No matter what get the metrics for the current run (since its an aggregate).
    getMetricsForLatestRunId(false);
    checkAndPollForMetrics.call(this);

    function getMetricsForLatestRunId(isPoll) {
      var latestRunId = DetailRunsStore.getLatestMetricRunId();
      if (!latestRunId) {
        return;
      }
      if (latestRunId === currentRunId) {
        return;
      }
      currentRunId = latestRunId;
      var appParams = angular.copy(DetailRunsStore.getParams());
      var logsParams = angular.copy(DetailRunsStore.getLogsParams());
      var metricParams = {
        namespace: appParams.namespace,
        app: appParams.app,
        run: latestRunId
      };
      var programType = DetailRunsStore.getMetricProgramType();
      metricParams[programType] = logsParams.programId;
      if (metricParams.run) {
        if (isPoll) {
          PipelineDetailMetricsActionFactory.pollForMetrics(metricParams);
        } else {
          PipelineDetailMetricsActionFactory.requestForMetrics(metricParams);
        }
      }
    }

    $scope.$on('$destroy', function() {
      PipelineDetailMetricsActionFactory.reset();
    });
  });
