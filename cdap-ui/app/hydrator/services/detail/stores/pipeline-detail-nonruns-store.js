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
  .service('HydratorPlusPlusDetailNonRunsStore', function(HydratorPlusPlusDetailDispatcher, HydratorPlusPlusHydratorService, myHelpers, HYDRATOR_DEFAULT_VALUES, MY_CONFIG, GLOBALS) {
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.HYDRATOR_DEFAULT_VALUES = HYDRATOR_DEFAULT_VALUES;
    this.myHelpers = myHelpers;
    this.isDistributed = MY_CONFIG.isEnterprise ? true : false;

    this.setDefaults = function(app) {
      this.state = {
        scheduleStatus: null,
        name: app.name || '',
        type: app.type,
        description: app.description,

        datasets: app.datasets || [],
        streams: app.streams || [],

        configJson: app.configJson || {},
        cloneConfig: app.cloneConfig || {},
        DAGConfig: app.DAGConfig || {nodes: [], connections: []}
      };
    };
    this.changeListeners = [];
    var dispatcher = HydratorPlusPlusDetailDispatcher.getDispatcher();

    this.setDefaults({});
    this.getScheduleStatus = function() {
      return this.state.scheduleStatus;
    };

    this.registerOnChangeListener = function(callback) {
      let index = this.changeListeners.push(callback);
      // un-subscribe for listners.
      return () => {
        this.changeListeners.splice(index-1, 1);
      };
    };
    this.emitChange = function() {
      this.changeListeners.forEach(function(callback) {
        callback(this.state);
      }.bind(this));
    };
    this.setState = function(schedule) {
      this.state.scheduleStatus = schedule.status || schedule;
      this.emitChange();
    };
    this.getCloneConfig = function() {
      return angular.copy(this.state.cloneConfig);
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
    this.getDAGConfig = function() {
      var config = angular.copy(this.state.DAGConfig);
      angular.forEach(config.nodes, (node) => {
        if (node.plugin){
          node.label = node.plugin.label;
        }
      });
      return config;
    };
    this.getConnections = function() {
      return this.state.DAGConfig.connections;
    };
    this.getNodes = function() {
      return this.state.DAGConfig.nodes;
    };
    this.getSourceNodes = function(nodeId) {
      let nodesMap = {};
      this.state.DAGConfig.nodes.forEach( node => nodesMap[node.name] = node );
      return this.state.DAGConfig.connections.filter( conn => conn.to === nodeId ).map( matchedConnection => nodesMap[matchedConnection.from] );
    };
    this.getDatasets = function() {
      return this.state.datasets;
    };
    this.getStreams = function() {
      return this.state.streams;
    };
    this.getConfigJson = function() {
      return this.state.configJson;
    };
    this.getAppType = function() {
      return this.state.type;
    };
    this.getPluginObject = function(nodeId) {
      var nodes = this.getNodes();
      var match = nodes.filter( node => node.name === nodeId);
      match = (match.length? match[0]: null);
      return match;
    };
    this.getArtifact = function() {
      return this.getCloneConfig().artifact;
    };
    this.getSchedule = function() {
      return this.getCloneConfig().config.schedule;
    };
    this.setSchedule = function(schedule) {
      this.state.cloneConfig.config.schedule = schedule;
    };
    this.getEngine = function() {
      return this.getCloneConfig().config.engine;
    };
    this.setEngine = function(engine) {
      this.state.cloneConfig.config.engine = engine;
    };
    this.getPostActions = function() {
      return this.getCloneConfig().config.postActions;
    };
    this.getBatchInterval = function() {
      return this.getCloneConfig().config.batchInterval;
    };
    this.setBatchInterval = function(batchInterval) {
      this.state.cloneConfig.config.batchInterval = batchInterval;
    };
    this.getInstance = function() {
      return this.getCloneConfig().config.instances;
    };
    this.getDriverMemoryMB = function() {
      return this.myHelpers.objectQuery(this.getCloneConfig(), 'config', 'driverResources', 'memoryMB');
    };
    this.setDriverMemoryMB = function(driverMemoryMB) {
      this.state.cloneConfig.config.driverResources.memoryMB = driverMemoryMB;
    };
    this.getDriverVirtualCores = function() {
      return this.myHelpers.objectQuery(this.getCloneConfig(), 'config', 'driverResources', 'virtualCores');
    };
    this.setDriverVirtualCores = function(driverVirtualCores) {
      this.state.cloneConfig.config.driverResources.virtualCores = driverVirtualCores;
    };
    this.getMemoryMB = function() {
      return this.myHelpers.objectQuery(this.getCloneConfig(), 'config', 'resources', 'memoryMB');
    };
    this.setMemoryMB = function(memoryMB) {
      this.state.cloneConfig.config.resources.memoryMB = memoryMB;
    };
    this.getVirtualCores = function() {
      return this.myHelpers.objectQuery(this.getCloneConfig(), 'config', 'resources', 'virtualCores');
    };
    this.setVirtualCores = function(virtualCores) {
      this.state.cloneConfig.config.resources.virtualCores = virtualCores;
    };
    this.getClientMemoryMB = function() {
      return this.myHelpers.objectQuery(this.getCloneConfig(), 'config', 'clientResources', 'memoryMB');
    };
    this.setClientMemoryMB = function(clientMemoryMB) {
      this.state.cloneConfig.config.clientResources.memoryMB = clientMemoryMB;
    };
    this.getClientVirtualCores = function() {
      return this.myHelpers.objectQuery(this.getCloneConfig(), 'config', 'clientResources', 'virtualCores');
    };
    this.setClientVirtualCores = function(clientVirtualCores) {
      this.state.cloneConfig.config.clientResources.virtualCores = clientVirtualCores;
    };
    this.getBackpressure = function() {
      return this.myHelpers.objectQuery(this.getCloneConfig(), 'config', 'properties', 'system.spark.spark.streaming.backpressure.enabled');
    };
    this.setBackpressure = function(backpressure) {
      this.state.cloneConfig.config.properties['system.spark.spark.streaming.backpressure.enabled'] = backpressure;
    };
    this.getProperties = function() {
      return this.myHelpers.objectQuery(this.getCloneConfig(), 'config', 'properties') || {};
    };
    this.getCustomConfig = function() {
      let customConfig = {};
      let currentProperties = this.getProperties();
      let backendProperties = ['system.spark.spark.streaming.backpressure.enabled', 'system.spark.spark.executor.instances', 'system.spark.spark.master'];
      for (let key in currentProperties) {
        if (currentProperties.hasOwnProperty(key) && backendProperties.indexOf(key) === -1) {
          customConfig[key] = currentProperties[key];
        }
      }
      return customConfig;
    };
    this.getCustomConfigForDisplay = function() {
      let currentCustomConfig = this.getCustomConfig();
      let customConfigForDisplay = {};
      for (let key in currentCustomConfig) {
        if (currentCustomConfig.hasOwnProperty(key)) {
          let newKey = key;
          if (this.getEngine() === 'mapreduce' && key.startsWith('system.mapreduce.')) {
            newKey = newKey.slice(17);
          } else if (key.startsWith('system.spark.')) {
            newKey = newKey.slice(13);
          }
          customConfigForDisplay[newKey] = currentCustomConfig[key];
        }
      }
      return customConfigForDisplay;
    };
    this.setCustomConfig = function(customConfig) {
      // have to do this because oldCustomConfig is already part of this.state.config.properties
      let oldCustomConfig = this.getCustomConfig();
      let oldProperties = this.getProperties();
      angular.forEach(oldCustomConfig, function(oldValue, oldKey) {
        if (oldProperties.hasOwnProperty(oldKey)) {
          delete oldProperties[oldKey];
        }
      });

      let newCustomConfig = {};
      angular.forEach(customConfig, function(configValue, configKey) {
        let newKey = configKey;
        if (GLOBALS.etlBatchPipelines.indexOf(this.state.cloneConfig.artifact.name) !== -1 && this.getEngine() === 'mapreduce') {
          newKey = 'system.mapreduce.' + configKey;
        } else {
          newKey = 'system.spark.' + configKey;
        }
        newCustomConfig[newKey] = configValue;
      }, this);

      let newProperties = angular.extend(oldProperties, newCustomConfig);
      this.state.cloneConfig.config.properties = newProperties;
    };
    this.getNumExecutors = function() {
      if (this.isDistributed) {
        return this.myHelpers.objectQuery(this.getCloneConfig(), 'config', 'properties', 'system.spark.spark.executor.instances');
      } else {
        // format on standalone is 'local[{number}]'
        let formattedNum = this.myHelpers.objectQuery(this.state.cloneConfig, 'config', 'properties', 'system.spark.spark.master');
        if (formattedNum) {
          return formattedNum.substring(6, formattedNum.length - 1);
        } else {
          return '1';
        }
      }
    };
    this.setNumExecutors = function(numExecutors) {
      if (this.isDistributed) {
        this.state.cloneConfig.config.properties['system.spark.spark.executor.instances'] = numExecutors;
      } else {
        this.state.cloneConfig.config.properties['system.spark.spark.master'] = `local[${numExecutors}]`;
      }
    };
    this.getInstrumentation = function() {
      return this.getCloneConfig().config.processTimingEnabled;
    };
    this.setInstrumentation = function(instrumentation) {
      this.state.cloneConfig.config.processTimingEnabled = instrumentation;
    };
    this.getStageLogging = function() {
      return this.getCloneConfig().config.stageLoggingEnabled;
    };
    this.setStageLogging = function(stageLogging) {
      this.state.cloneConfig.config.stageLoggingEnabled = stageLogging;
    };
    this.getCheckpointing = function() {
      return this.getCloneConfig().config.disableCheckpoints;
    };
    this.setCheckpointing = function(checkpointing) {
      this.state.cloneConfig.config.disableCheckpoints = checkpointing;
    };
    this.getNumRecordsPreview = function() {
      return this.getCloneConfig().config.numOfRecordsPreview;
    };
    this.setNumRecordsPreview = function(numRecordsPreview) {
      this.state.cloneConfig.config.numOfRecordsPreview = numRecordsPreview;
    };
    this.getMaxConcurrentRuns = function() {
      return this.getCloneConfig().config.maxConcurrentRuns;
    };
    this.setMaxConcurrentRuns = function(maxConcurrentRuns) {
      this.state.cloneConfig.config.maxConcurrentRuns = maxConcurrentRuns;
    };

    this.getNode = this.getPluginObject;
    this.init = function(app) {
      var appConfig = {};
      var uiConfig;
      angular.extend(appConfig, app);

      try {
        appConfig.configJson = JSON.parse(app.configuration);
      } catch(e) {
        appConfig.configJson = e;
        console.log('ERROR cannot parse configuration');
        return;
      }
      if(appConfig.configJson) {
        app.config = appConfig.configJson;
        uiConfig = this.HydratorPlusPlusHydratorService.getNodesAndConnectionsFromConfig(app);

        appConfig.DAGConfig = {
          nodes: uiConfig.nodes,
          connections: uiConfig.connections
        };

        appConfig.description = appConfig.configJson.description ? appConfig.configJson.description : appConfig.description;
      }

      appConfig.type = app.artifact.name;

      // FIXME: TL;DR - Object reference being changed somewhere in the detailed view is affecting the clone behavior.
      // Longer version -
      // Right now appConfig.configJson is modified somewhere (modify reference) which affects the 'schema' property of 'Stream'
      // plugin once the node is opened in the bottom panel. This is causing clone to fail. I am creating a copy so that no matter
      // what gets changed in the detailed view is not passed on to the clone. This shouldn't be a problem as nothing should be changed
      // in detailed view. `appConfig.configJson` is the culprit.
      // One of the worst cases of 2way binding where right now the app is super big that I have no f***ing clue where which one is modified.
      let appConfigClone = angular.copy(appConfig);
      appConfig.cloneConfig = {
        name: app.name,
        artifact: app.artifact,
        description: appConfigClone.configJson.description,
        __ui__: appConfigClone.DAGConfig,
        config: {
          instances: appConfigClone.configJson.instances,
          batchInterval: appConfigClone.configJson.batchInterval,
          resources: appConfigClone.configJson.resources,
          driverResources: appConfigClone.configJson.driverResources,
          clientResources: appConfigClone.configJson.clientResources,
          schedule: appConfigClone.configJson.schedule,
          connections: uiConfig.connections,
          comments: appConfigClone.configJson.comments,
          postActions: appConfigClone.configJson.postActions,
          engine: appConfigClone.configJson.engine,
          stages: appConfigClone.configJson.stages,
          properties: appConfigClone.configJson.properties,
          processTimingEnabled: appConfigClone.configJson.processTimingEnabled,
          stageLoggingEnabled: appConfigClone.configJson.stageLoggingEnabled,
          disableCheckpoints: appConfigClone.configJson.disableCheckpoints,
          stopGracefully: appConfigClone.configJson.stopGracefully,
          maxConcurrentRuns: appConfigClone.configJson.maxConcurrentRuns
        }
      };
      appConfig.streams = app.streams.map(function (stream) {
        stream.type = 'Stream';
        return stream;
      });
      appConfig.datasets = app.datasets.map(function (dataset) {
        dataset.type = 'Dataset';
        return dataset;
      });
      this.setDefaults(appConfig);
    };
    this.reset = function() {
      this.setDefaults({});
      this.changeListeners = [];
    };
    dispatcher.register('onScheduleStatusFetch', this.setState.bind(this));
    dispatcher.register('onScheduleStatusFetchFail', this.setState.bind(this, {error: 'Failed to fetch schedule for the pipeline.'}));
    dispatcher.register('onReset', this.reset.bind(this));
  });
