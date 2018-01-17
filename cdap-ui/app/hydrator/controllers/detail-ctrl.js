/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
  .controller('HydratorPlusPlusDetailCtrl', function(rPipelineDetail, $scope, HydratorPlusPlusDetailMetricsActions, $stateParams, PipelineAvailablePluginsActions, GLOBALS) {
    // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
    const actionCreator = window.CaskCommon.PipelineDetailActionCreator;
    this.pipelineType = rPipelineDetail.artifact.name;
    let programType = this.pipelineType === GLOBALS.etlDataPipeline ? 'workflows' : 'spark';
    let programName = this.pipelineType === GLOBALS.etlDataPipeline ? 'DataPipelineWorkflow' : 'DataStreamsSparkStreaming';
    let scheduleId = GLOBALS.defaultScheduleId;

    let currentRunId;
    let pluginsFetched = false;

    actionCreator.init(rPipelineDetail);
    let runid = $stateParams.runid;
    if (runid) {
      actionCreator.setCurrentRun(runid);
    }

    actionCreator.getRuns({
      namespace: $stateParams.namespace,
      appId: rPipelineDetail.name,
      programType,
      programName
    });

    actionCreator.fetchScheduleStatus({
      namespace: $stateParams.namespace,
      appId: rPipelineDetail.name,
      scheduleId
    });

    let pipelineDetailStoreSubscription = window.CaskCommon.PipelineDetailStore.subscribe(() => {
      let pipelineDetailStoreState = window.CaskCommon.PipelineDetailStore.getState();

      if (!pluginsFetched) {
        PipelineAvailablePluginsActions.fetchPluginsForDetails($stateParams.namespace, pipelineDetailStoreState.config.stages);
        pluginsFetched = true;
      }

      let latestRun = pipelineDetailStoreState.currentRun;
      let latestRunId = latestRun.runid;

      if (!latestRunId || currentRunId === latestRunId) {
        return;
      }

      currentRunId = latestRunId;
      // When current run id changes reset the metrics in the DAG.
      HydratorPlusPlusDetailMetricsActions.reset();

      let metricProgramType = programType === 'workflows' ? 'workflow' : programType;

      let metricParams = {
        namespace: $stateParams.namespace,
        app: rPipelineDetail.name,
        run: latestRunId,
        [metricProgramType]: programName
      };

      if (latestRun.status !== 'RUNNING') {
        HydratorPlusPlusDetailMetricsActions.requestForMetrics(metricParams);
      } else {
        HydratorPlusPlusDetailMetricsActions.pollForMetrics(metricParams);
      }
    });

    $scope.$on('$destroy', function() {
      // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
      actionCreator.reset();
      pipelineDetailStoreSubscription();
      HydratorPlusPlusDetailMetricsActions.reset();
    });
  });
