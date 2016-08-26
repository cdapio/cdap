/*
 * Copyright © 2016 Cask Data, Inc.
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
  .controller('HydratorPlusPlusDetailCtrl', function(HydratorPlusPlusDetailRunsStore, rPipelineDetail, HydratorPlusPlusDetailActions, $scope, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailMetricsActions) {
    // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
    HydratorPlusPlusDetailRunsStore.init(rPipelineDetail);
    HydratorPlusPlusDetailNonRunsStore.init(rPipelineDetail);

    var params = HydratorPlusPlusDetailRunsStore.getParams();
    params.scope = $scope;
    var currentRunId;

    HydratorPlusPlusDetailRunsStore.registerOnChangeListener(function () {

      var latestRunId = HydratorPlusPlusDetailRunsStore.getLatestMetricRunId();

      if (currentRunId === latestRunId) {
        return;
      }

      currentRunId = latestRunId;
      // When current run id changes reset the metrics in the DAG.
      HydratorPlusPlusDetailMetricsActions.reset();

      if (latestRunId) {
        var appParams = HydratorPlusPlusDetailRunsStore.getParams();
        var logsParams = HydratorPlusPlusDetailRunsStore.getLogsParams();
        var metricParams = {
          namespace: appParams.namespace,
          app: appParams.app,
          run: latestRunId
        };
        var programType = HydratorPlusPlusDetailRunsStore.getMetricProgramType();
        metricParams[programType] = logsParams.programId;

        HydratorPlusPlusDetailMetricsActions.pollForMetrics(metricParams);
      }

    });

    HydratorPlusPlusDetailActions.pollRuns(
      HydratorPlusPlusDetailRunsStore.getApi(),
      params
    );

    $scope.$on('$destroy', function() {
      // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
      HydratorPlusPlusDetailRunsStore.reset();
      HydratorPlusPlusDetailNonRunsStore.reset();
      HydratorPlusPlusDetailMetricsActions.reset();
    });
  });
