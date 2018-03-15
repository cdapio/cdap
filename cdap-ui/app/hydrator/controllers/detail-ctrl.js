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
  .controller('HydratorPlusPlusDetailCtrl', function(rPipelineDetail, $scope, $stateParams, PipelineAvailablePluginsActions, GLOBALS) {
    // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
    const pipelineDetailsActionCreator = window.CaskCommon.PipelineDetailActionCreator;
    const pipelineMetricsActionCreator = window.CaskCommon.PipelineMetricsActionCreator;

    this.pipelineType = rPipelineDetail.artifact.name;
    let programType = this.pipelineType === GLOBALS.etlDataPipeline ? 'workflows' : 'spark';
    let programName = this.pipelineType === GLOBALS.etlDataPipeline ? 'DataPipelineWorkflow' : 'DataStreamsSparkStreaming';
    let scheduleId = GLOBALS.defaultScheduleId;

    let currentRun, metricsObservable;
    let pluginsFetched = false;

    pipelineDetailsActionCreator.init(rPipelineDetail);
    let runid = $stateParams.runid;
    if (runid) {
      pipelineDetailsActionCreator.setCurrentRunId(runid);
    }

    let runsPoll = pipelineDetailsActionCreator.pollRuns({
      namespace: $stateParams.namespace,
      appId: rPipelineDetail.name,
      programType,
      programName
    });

    pipelineDetailsActionCreator.fetchScheduleStatus({
      namespace: $stateParams.namespace,
      appId: rPipelineDetail.name,
      scheduleId
    });

    let pipelineDetailStoreSubscription = window.CaskCommon.PipelineDetailStore.subscribe(() => {
      let pipelineDetailStoreState = window.CaskCommon.PipelineDetailStore.getState();

      if (!pluginsFetched) {
        let pluginsToFetchDetailsFor = pipelineDetailStoreState.config.stages.concat(pipelineDetailStoreState.config.postActions);
        PipelineAvailablePluginsActions.fetchPluginsForDetails($stateParams.namespace, pluginsToFetchDetailsFor);
        pluginsFetched = true;
      }

      let latestRun = pipelineDetailStoreState.currentRun;
      if (!latestRun || !latestRun.runid) {
        return;
      }

      // let latestRunId = latestRun.runid;
      if (currentRun && currentRun.runid === latestRun.runid && currentRun.status === latestRun.status) {
        return;
      }

      // When current run id changes reset the metrics in the DAG.
      if (currentRun && currentRun.runid !== latestRun.runid) {
        pipelineMetricsActionCreator.reset();
      }

      currentRun = latestRun;

      let metricProgramType = programType === 'workflows' ? 'workflow' : programType;

      let metricParams = {
        namespace: $stateParams.namespace,
        app: rPipelineDetail.name,
        run: latestRun.runid,
        [metricProgramType]: programName
      };

      if (metricsObservable) {
        metricsObservable.unsubscribe();
      }

      if (latestRun.status !== 'RUNNING') {
        pipelineMetricsActionCreator.getMetrics(metricParams);
      } else {
        metricsObservable = pipelineMetricsActionCreator.pollForMetrics(metricParams);
      }
    });

    $scope.$on('$destroy', function() {
      // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
      if (runsPoll) {
        runsPoll.unsubscribe();
      }
      if (metricsObservable) {
        metricsObservable.unsubscribe();
      }
      pipelineDetailsActionCreator.reset();
      pipelineDetailStoreSubscription();
      pipelineMetricsActionCreator.reset();
    });
  });
