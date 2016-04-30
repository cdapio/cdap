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

angular.module(PKG.name + '.feature.hydratorplusplus')
  .service('HydratorPlusPlusDetailRunsStore', function(HydratorPlusPlusDetailDispatcher, $state, myHelpers, GLOBALS, myPipelineCommonApi, HydratorService) {

    var dispatcher = HydratorPlusPlusDetailDispatcher.getDispatcher();
    this.changeListeners = [];
    this.HydratorService = HydratorService;
    this.setDefaults = function(app) {
      this.state = {
        runs:{
          list: [],
          latest: {},
          count: 0,
          nextRunTime: null
        },
        params: app.params || {},
        scheduleParams: app.scheduleParams || {},
        logsParams: app.logsParams || {},
        api: app.api,
        type: app.type,
        metricProgramType: app.metricProgramType,
        statistics: ''
      };
    };
    this.setDefaults({});

    this.getRuns = function() {
      return angular.copy(this.state.runs.list);
    };
    this.getLatestRun = function() {
      return this.state.runs.list[0];
    };
    this.getLatestMetricRunId = function() {
      var metricRunId;
      if (!this.state.runs.count) {
        return false;
      }
      metricRunId = this.state.runs.list[0].runid;
      return metricRunId;
    };
    this.getAppType = function() {
      return this.state.type;
    };
    this.getStatus = function() {
      var status;
      if (this.state.runs.list.length === 0) {
        status = 'STOPPED';
      } else {
        status = myHelpers.objectQuery(this.state, 'runs', 'latest', 'status') || '';
      }
      return status;
    };
    this.getRunsCount = function() {
      return this.state.runs.count;
    };
    this.getNextRunTime = function() {
      return this.state.runs.nextRunTime;
    };
    this.setNextRunTime = function(nextRunTime) {
      this.state.runs.nextRunTime = nextRunTime;
      this.emitChange();
    };
    this.getApi = function() {
      return this.state.api;
    };
    this.getParams = function() {
      return this.state.params;
    };
    this.getScheduleParams = function() {
      return this.state.scheduleParams;
    };
    this.getLogsParams = function() {
      var logsParams = angular.extend({runId: this.state.runs.latest.runid}, this.state.logsParams);
      return logsParams;
    };

    this.getStatistics = function() {
      return this.state.statistics;
    };

    this.getMetricProgramType = function() {
      return this.state.metricProgramType;
    };

    this.registerOnChangeListener = function(callback) {
      this.changeListeners.push(callback);
    };
    this.emitChange = function() {
      this.changeListeners.forEach(function(callback) {
        callback();
      });
    };

    this.setRunsState = function(runs) {
      if (!runs.length) {
        return;
      }
      this.state.runs = {
        list: runs,
        count: runs.length,
        latest: runs[0],
        runsCount: runs.length,
        nextRunTime: this.state.runs.nextRunTime || null
      };
      this.state.logsParams.runId = this.state.runs.latest.runid;
      this.emitChange();
    };
    this.setStatistics = function(statistics) {
      this.state.statistics = statistics;
      this.emitChange();
    };
    this.init = function(app) {
      var appConfig = {};
      var appLevelParams,
          logsLevelParams,
          metricProgramType,
          programType;

      angular.extend(appConfig, app);
      appLevelParams = {
        namespace: $state.params.namespace,
        app: app.name
      };

      logsLevelParams = {
        namespace: $state.params.namespace,
        appId: app.name
      };

      programType = GLOBALS.etlBatchPipelines.indexOf(app.artifact.name) !== -1 ? 'WORKFLOWS' : 'WORKER';

      if (programType === 'WORKFLOWS') {
        angular.forEach(app.programs, function (program) {
          if (program.type === 'Workflow') {
            appLevelParams.programName = program.id;
            appLevelParams.programType = program.type.toLowerCase() + 's';
            metricProgramType = program.type.toLowerCase();
            logsLevelParams.programId = program.id;
            logsLevelParams.programType = appLevelParams.programType;
          }
        });
      } else {
        angular.forEach(app.programs, function (program) {
          metricProgramType = program.type.toLowerCase();
          appLevelParams.programName = program.id;
          appLevelParams.programType = program.type.toLowerCase() + 's';

          logsLevelParams.programId = program.id;
          logsLevelParams.programType = program.type.toLowerCase() + 's';
        });
      }
      appConfig.type = app.artifact.name;
      appConfig.logsParams = logsLevelParams;
      appConfig.params = appLevelParams;
      appConfig.api = myPipelineCommonApi;
      appConfig.metricProgramType = metricProgramType;
      appConfig.scheduleParams = {
        app: appLevelParams.app,
        schedule: appConfig.type === GLOBALS.etlDataPipeline ? 'dataPipelineSchedule' : 'etlWorkflow',
        namespace: appLevelParams.namespace
      };
      this.setDefaults(appConfig);
    };
    this.reset = function() {
      this.setDefaults({});
      this.changeListeners = [];
    };

    dispatcher.register('onRunsChange', this.setRunsState.bind(this));
    dispatcher.register('onStatisticsFetch', this.setStatistics.bind(this));
    dispatcher.register('onReset', this.setDefaults.bind(this, {}));
    dispatcher.register('onNextRunTime', this.setNextRunTime.bind(this));
  });
