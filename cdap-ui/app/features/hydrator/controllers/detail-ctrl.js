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
  .controller('HydratorDetailController', function(DetailRunsStore, rPipelineDetail, PipelineDetailActionFactory, $scope, DetailNonRunsStore, NodeConfigStore, PipelineDetailMetricsActionFactory) {
    // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
    DetailRunsStore.init(rPipelineDetail);
    DetailNonRunsStore.init(rPipelineDetail);
    NodeConfigStore.init();

    var params = angular.copy(DetailRunsStore.getParams());
    params.scope = $scope;
    var currentRunId;

    DetailRunsStore.registerOnChangeListener(function () {

      var latestRunId = DetailRunsStore.getLatestMetricRunId();

      if (currentRunId === latestRunId) {
        return;
      }

      currentRunId = latestRunId;

      if (latestRunId) {
        var appParams = angular.copy(DetailRunsStore.getParams());
        var logsParams = angular.copy(DetailRunsStore.getLogsParams());
        var metricParams = {
          namespace: appParams.namespace,
          app: appParams.app,
          run: latestRunId
        };
        var programType = DetailRunsStore.getMetricProgramType();
        metricParams[programType] = logsParams.programId;

        PipelineDetailMetricsActionFactory.pollForMetrics(metricParams);
      }

    });

    PipelineDetailActionFactory.pollRuns(
      DetailRunsStore.getApi(),
      params
    );

    $scope.$on('$destroy', function() {
      // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
      DetailRunsStore.reset();
      DetailNonRunsStore.reset();
      NodeConfigStore.reset();
      PipelineDetailMetricsActionFactory.reset();
    });
  });
