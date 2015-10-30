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
  .service('DetailRunsStore', function(PipelineDetailDispatcher, $state, myHelpers, GLOBALS, myWorkFlowApi, myWorkersApi) {

    var dispatcher = PipelineDetailDispatcher.getDispatcher();
    this.changeListeners = [];
    this.setDefaults = function(app) {
      this.state = {
        runs:{
          list: [],
          latest: {},
          count: 0,
        },
        params: app.params || {},
        scheduleParams: app.scheduleParams || {},
        logsParams: app.logsParams || {},
        configJson: app.configJson || {},
        api: app.api,
        type: app.type,
        metricProgramType: app.metricProgramType,
        statistics: '',
        name: app.name,
        description: app.description
      };
    };
    this.setDefaults({});

    this.changeListeners = [];

    this.getRuns = function() {
      return this.state.runs.list;
    };
    this.getLatestRun = function() {
      return this.state.runs.list[0];
    };
    this.getHistory = this.getRuns;

    this.getStatus = function() {
      return myHelpers.objectQuery(this.state, 'runs', 'latest', 'status') || '';
    };
    this.getRunsCount = function() {
      return this.state.runs.count;
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
      return this.state.logsParams;
    };
    this.getConfigJson = function() {
      return this.state.configJson;
    };
    this.getPipelineType = function() {
      return this.state.type;
    };
    this.getPipelineName = function() {
      return this.state.name;
    };
    this.getPipelineDescription = function() {
      return this.state.description;
    };
    this.getStatistics = function() {
      return this.state.statistics;
    };
    this.getAppType = function() {
      return this.state.type;
    };
    this.getMetricProgramType = function() {
      return this.state.metricProgramType;
    };
    this.registerOnChangeListener = function(callback) {
      this.changeListeners.push(callback);
    };
    this.emitChange = function() {
      this.changeListeners.forEach(function(callback) {
        callback(this.state);
      });
    };

    this.setRunsState = function(runs) {
      if (runs[0].runid === myHelpers.objectQuery(this.state, 'runs', 'latest', 'runid')) {
        return;
      }
      this.state.runs = {
        list: runs,
        count: runs.length,
        latest: runs[0],
        runsCount: runs.length
      };
      if (this.state.type === GLOBALS.etlBatch) {
        this.state.logsParams.runId = this.state.runs.latest.properties.ETLMapReduce;
      } else {
        this.state.logsParams.runId = this.state.runs.latest.runid;
      }
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
          api,
          programType;

      angular.extend(appConfig, app);
      try {
        appConfig.configJson = JSON.parse(app.configuration);
      } catch(e) {
        appConfig.configJson = e;
        console.log('ERROR cannot parse configuration');
        return;
      }
      appLevelParams = {
        namespace: $state.params.namespace,
        appId: app.name
      };

      logsLevelParams = angular.copy(appLevelParams);
      programType = app.artifact.name === GLOBALS.etlBatch ? 'WORKFLOWS' : 'WORKER';

      if (programType === 'WORKFLOWS') {
        api = myWorkFlowApi;
        angular.forEach(app.programs, function (program) {
          if (program.type === 'Workflow') {
            appLevelParams.workflowId = program.id;
          } else {
            metricProgramType = 'mapreduce';
            logsLevelParams.programId = program.id;
            logsLevelParams.programType = program.type.toLowerCase();
          }
        });
      } else {
        api = myWorkersApi;
        angular.forEach(app.programs, function (program) {
          metricProgramType = 'worker';
          appLevelParams.workerId = program.id;
          logsLevelParams.programId = program.id;
          logsLevelParams.programType = program.type.toLowerCase() + 's';
        });
      }

      appConfig.logsParams = logsLevelParams;
      appConfig.params = appLevelParams;
      appConfig.api = api;
      appConfig.metricProgramType = metricProgramType;
      appConfig.type = app.artifact.name;
      appConfig.scheduleParams = angular.extend({scheduleId: 'etlWorkflow'}, appLevelParams);
      this.setDefaults(appConfig);
    };

    dispatcher.register('onRunsChange', this.setRunsState.bind(this));
    dispatcher.register('onStatisticsFetch', this.setStatistics.bind(this));
  });
