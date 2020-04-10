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
  .controller('HydratorPlusPlusDetailCtrl', function(rPipelineDetail, $scope, $stateParams, PipelineAvailablePluginsActions, GLOBALS, myHelpers) {
    // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
    const pipelineDetailsActionCreator = window.CaskCommon.PipelineDetailActionCreator;
    const pipelineMetricsActionCreator = window.CaskCommon.PipelineMetricsActionCreator;
    const pipelineConfigurationsActionCreator = window.CaskCommon.PipelineConfigurationsActionCreator;

    this.pipelineType = rPipelineDetail.artifact.name;
    let programType = GLOBALS.programType[this.pipelineType];
    let programTypeForRunsCount = GLOBALS.programTypeForRunsCount[this.pipelineType];
    let programName = GLOBALS.programId[this.pipelineType];
    let scheduleId = GLOBALS.defaultScheduleId;

    let currentRun, metricsObservable, runsPoll, runsCountPoll;
    let pluginsFetched = false;

    pipelineDetailsActionCreator.init(rPipelineDetail);
    let runid = $stateParams.runid;

    this.eventEmitter = window.CaskCommon.ee(window.CaskCommon.ee);
    this.pageLevelError = null;
    const { globalEvents } = window.CaskCommon;

    this.eventEmitter.on(globalEvents.PAGE_LEVEL_ERROR, (error) => {
      if (error.reset === true) {
        this.pageLevelError = null;
      }
      else {
        this.pageLevelError = myHelpers.handlePageLevelError(error);
      }
    });

    let runsFetch = pipelineDetailsActionCreator.getRuns({
      namespace: $stateParams.namespace,
      appId: rPipelineDetail.name,
      programType,
      programName
    });

    runsFetch.subscribe(() => {
      let {runs} = window.CaskCommon.PipelineDetailStore.getState();
      let doesCurrentRunExists = _.find(runs, (run) => run.runid === runid);
      /**
       * We do this here because of this usecase,
       *
       * 1. User goes to pipeline which has 130 runs
       * 2. Opens up summary and clicks on the 30th run from the runs history graph
       * 3. User is now at 30 of 130 runs
       * 4. User starts more new runs
       * 5. At later point when the user refreshes the UI, the current run id in the url won't be in the latest 100 runs
       *
       * So instead of having a runid in the url and showing the information of latest run (which is incorrect) we fetch
       * the run detail and add it to the runs.
       *
       * This will render the run number incorrect but its ok compared to the whole run information being incorrect.
       */
      if (runid && !doesCurrentRunExists) {
        pipelineDetailsActionCreator
          .getRunDetails({
            namespace: $stateParams.namespace,
            appId: rPipelineDetail.name,
            programType,
            programName,
            runid
          })
          .subscribe(runDetails => {
            let {runs} = window.CaskCommon.PipelineDetailStore.getState();
            runs.push(runDetails);
            pipelineDetailsActionCreator.setCurrentRunId(runid);
            pipelineDetailsActionCreator.setRuns(runs);
          });
      } else if (runid) {
        pipelineDetailsActionCreator.setCurrentRunId(runid);
      }
      pollRuns();
    });

    pollRunsCount();

    function pollRuns() {
      runsPoll = pipelineDetailsActionCreator.pollRuns({
        namespace: $stateParams.namespace,
        appId: rPipelineDetail.name,
        programType,
        programName
      });
    }

    function pollRunsCount() {
      runsCountPoll = pipelineDetailsActionCreator.pollRunsCount({
        namespace: $stateParams.namespace,
        appId: rPipelineDetail.name,
        programType: programTypeForRunsCount,
        programName
      });
    }

    pipelineDetailsActionCreator.fetchScheduleStatus({
      namespace: $stateParams.namespace,
      appId: rPipelineDetail.name,
      scheduleId
    });

    let pipelineDetailStoreSubscription = window.CaskCommon.PipelineDetailStore.subscribe(() => {
      let pipelineDetailStoreState = window.CaskCommon.PipelineDetailStore.getState();

      if (!pluginsFetched) {
        let pluginsToFetchDetailsFor = pipelineDetailStoreState.config.stages.concat(pipelineDetailStoreState.config.postActions || []);
        PipelineAvailablePluginsActions.fetchPluginsForDetails($stateParams.namespace, pluginsToFetchDetailsFor);
        pluginsFetched = true;
      }

      let latestRun = pipelineDetailStoreState.currentRun;
      if (!latestRun || !latestRun.runid) {
        return;
      }

      // let latestRunId = latestRun.runid;
      if (
        currentRun &&
        currentRun.runid === latestRun.runid &&
        currentRun.status === latestRun.status &&
        currentRun.status !== 'RUNNING'
      ) {
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

    this.eventEmitter.on(window.CaskCommon.WINDOW_ON_FOCUS, () => {
      pollRuns();
      pollRunsCount();
    });

    this.eventEmitter.on(window.CaskCommon.WINDOW_ON_BLUR, () => {
      if (metricsObservable) {
        metricsObservable.unsubscribe();
      }
      if (runsPoll) {
        runsPoll.unsubscribe();
      }
      if (runsCountPoll) {
        runsCountPoll.unsubscribe();
      }
    });


    $scope.$on('$destroy', function() {
      // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
      if (runsPoll) {
        runsPoll.unsubscribe();
      }
      if (runsCountPoll) {
        runsCountPoll.unsubscribe();
      }
      if (metricsObservable) {
        metricsObservable.unsubscribe();
      }
      pipelineConfigurationsActionCreator.reset();
      pipelineDetailsActionCreator.reset();
      pipelineDetailStoreSubscription();
      pipelineMetricsActionCreator.reset();
    });
  });
