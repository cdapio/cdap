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
        runId: (DetailRunsStore.getLatestRun() && DetailRunsStore.getLatestRun().runId) || null,
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
      var latestRunId = DetailRunsStore.getLatestRun().runid;
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
