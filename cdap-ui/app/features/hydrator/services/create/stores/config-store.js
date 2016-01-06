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

class ConfigStore {
  constructor(ConfigDispatcher, CanvasFactory, GLOBALS, mySettings, ConsoleActionsFactory, $stateParams, myHelpers, NonStorePipelineErrorFactory){
    this.state = {};
    this.mySettings = mySettings;
    this.ConsoleActionsFactory = ConsoleActionsFactory;
    this.CanvasFactory = CanvasFactory;
    this.GLOBALS = GLOBALS;
    this.$stateParams = $stateParams;
    this.myHelpers = myHelpers;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;

    this.changeListeners = [];
    this.setDefaults();
    this.configDispatcher = ConfigDispatcher.getDispatcher();
    this.configDispatcher.register('onArtifactSave', this.setArtifact.bind(this));
    this.configDispatcher.register('onMetadataInfoSave', this.setMetadataInformation.bind(this));
    this.configDispatcher.register('onPluginAdd', this.setConfig.bind(this));
    this.configDispatcher.register('onPluginEdit', this.editNodeProperties.bind(this));
    this.configDispatcher.register('onSetSchedule', this.setSchedule.bind(this));
    this.configDispatcher.register('onSetInstance', this.setInstance.bind(this));
    this.configDispatcher.register('onSaveAsDraft', this.saveAsDraft.bind(this));
    this.configDispatcher.register('onInitialize', this.init.bind(this));
  }
  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }
  setDefaults(config) {
    this.state = {
      artifact: {
        name: '',
        scope: 'SYSTEM',
        version: ''
      },
      __ui__: {
        nodes: [],
        isEditing: true
      },
      description: '',
      name: ''
    };
    angular.extend(this.state, {config: this.getDefaultConfig()});

    // This will be eventually used when we just pass on a config to the store to draw the dag.
    if (config) {
      angular.extend(this.state, config);
      this.setArtifact(this.state.artifact);
      this.setEngine(this.state.config.engine);
      this.propagateIOSchemas();
    }
  }
  init(config) {
    this.setDefaults(config);
  }
  getDefaultConfig() {
    return {
      source: {
        name: '',
        plugin: {}
      },
      sinks: [],
      transforms: [],
      connections: [],
      comments: []
    };
  }

  setState(state) {
    this.state = state;
  }
  getState() {
    return angular.copy(this.state);
  }
  getArtifact() {
    return this.getState().artifact;
  }
  getAppType() {
    return this.getState().artifact.name;
  }
  getConnections() {
    return this.getConfig().connections;
  }
  getConfig() {
    return this.getState().config;
  }
  generateConfigFromState() {
    var config = this.getDefaultConfig();
    var artifactTypeExtension = this.GLOBALS.pluginTypes[this.state.artifact.name];
    var nodesMap = {};
    this.state.__ui__.nodes.forEach(function(n) {
      nodesMap[n.name] = n;
    });

    function addPluginToConfig(node, id) {
      var pluginConfig =  {
        // Solely adding id and _backendProperties for validation.
        // Should be removed while saving it to backend.
        name: node.plugin.name,
        label: node.plugin.label,
        artifact: node.plugin.artifact,
        properties: node.plugin.properties,
        _backendProperties: node._backendProperties,
        outputSchema: node.outputSchema
      };

      if (node.type === artifactTypeExtension.source) {
        config['source'] = {
          name: node.plugin.label,
          plugin: pluginConfig
        };
      } else if (node.type === 'transform') {
        if (node.errorDatasetName && node.errorDatasetName.length > 0) {
          pluginConfig.errorDatasetName = node.errorDatasetName;
        }
        if (node.validationFields) {
          pluginConfig.validationFields = node.validationFields;
        }
        pluginConfig = {
          name: node.plugin.label,
          plugin: pluginConfig
        };
        config['transforms'].push(pluginConfig);
      } else if (node.type === artifactTypeExtension.sink) {
        pluginConfig = {
          name: node.plugin.label,
          plugin: pluginConfig
        };
        config['sinks'].push(pluginConfig);
      }
      delete nodesMap[id];
    }

    var connections = this.CanvasFactory.orderConnections(
      angular.copy(this.state.config.connections),
      this.state.artifact.name,
      this.state.__ui__.nodes
    );

    connections.forEach( connection => {
      let fromConnectionName, toConnectionName;

      if (nodesMap[connection.from]) {
        fromConnectionName = nodesMap[connection.from].plugin.label;
        addPluginToConfig(nodesMap[connection.from], connection.from);
      } else {
        fromConnectionName = this.state.__ui__.nodes.filter( n => n.name === connection.from)[0];
        fromConnectionName = fromConnectionName.plugin.label;
      }
      if (nodesMap[connection.to]) {
        toConnectionName = nodesMap[connection.to].plugin.label;
        addPluginToConfig(nodesMap[connection.to], connection.to);
      } else {
        toConnectionName = this.state.__ui__.nodes.filter( n => n.name === connection.to)[0];
        toConnectionName = toConnectionName.plugin.label;
      }
      connection.from = fromConnectionName;
      connection.to = toConnectionName;
    });
    config.connections = connections;

    let appType = this.getAppType();
    if ( appType=== this.GLOBALS.etlBatch) {
      config.schedule = this.getSchedule();
      config.engine = this.getEngine();
    } else if (appType === this.GLOBALS.etlRealtime) {
      config.instance = this.getInstance();
    }

    if (this.state.description) {
      config.description = this.state.description;
    }

    config.comments = this.getComments();

    return config;
  }
  getConfigForExport() {
    var state = this.getState();
    // Stripping of uuids and generating configs is what is going on here.

    var config = angular.copy(this.generateConfigFromState());
    this.CanvasFactory.pruneProperties(config);
    state.config = angular.copy(config);

    var nodes = angular.copy(this.getNodes()).map( node => {
      node.name = node.plugin.label;
      return node;
    });
    state.__ui__.nodes = nodes;

    return angular.copy(state);
  }
  getDisplayConfig() {
    let uniqueNodeNames = {};
    let ERROR_MESSAGES = this.GLOBALS.en.hydrator.studio.error;
    this.ConsoleActionsFactory.resetMessages();
    this.NonStorePipelineErrorFactory.isUniqueNodeNames(this.getNodes(), (err, node) => {
      if (err) {
        uniqueNodeNames[node.plugin.label] = err;
      }
    });

    if (Object.keys(uniqueNodeNames).length > 0) {
      angular.forEach(uniqueNodeNames, (err, nodeName) => {
        this.ConsoleActionsFactory.addMessage({
          type: 'error',
          content: nodeName + ': ' + ERROR_MESSAGES[err]
        });
      });
      return false;
    }
    var stateCopy = this.getConfigForExport();
    var source = stateCopy.config.source;
    var sinks = stateCopy.config.sinks;
    var transforms = stateCopy.config.transforms;

    if (source.plugin) {
      delete source.plugin.outputSchema;
    }
    angular.forEach(sinks, (sink) => {
      if (sink.plugin) {
        delete sink.plugin.outputSchema;
      }
    });
    angular.forEach(transforms, (transform) =>  {
      if (transform.plugin) {
        delete transform.plugin.outputSchema;
      }
    });
    delete stateCopy.__ui__;

    angular.forEach(stateCopy.config.comments, (comment) => {
      delete comment.isActive;
      delete comment.id;
      delete comment._uiPosition;
    });

    return stateCopy;
  }
  getDescription() {
    return this.getState().description;
  }
  getName() {
    return this.getState().name;
  }
  setName(name) {
    this.state.name = name;
    this.emitChange();
  }
  setDescription(description) {
    this.state.description = description;
    this.emitChange();
  }
  setMetadataInformation(name, description) {
    this.state.name = name;
    this.state.description = description;
    this.emitChange();
  }
  setConfig(config, type) {
    switch(type) {
      case 'source':
        this.state.config.source = config;
        break;
      case 'sink':
        this.state.config.sinks.push(config);
        break;
      case 'transform':
        this.state.config.transforms.push(config);
        break;
    }
    this.emitChange();
  }
  setEngine(engine) {
    if (this.state.config.artifact === this.GLOBALS.etlBatch) {
      this.state.config.engine = engine || 'mapreduce';
    }
  }
  getEngine() {
    return this.state.config.engine || 'mapreduce';
  }
  setArtifact(artifact) {
    this.state.artifact.name = artifact.name;
    this.state.artifact.version = artifact.version;
    this.state.artifact.scope = artifact.scope;

    if (artifact.name === this.GLOBALS.etlBatch) {
      this.state.config.schedule = '* * * * *';
    } else if (artifact.name === this.GLOBALS.etlRealtime) {
      this.state.config.instance = 1;
    }

    this.emitChange();
  }

  setNodes(nodes) {
    this.state.__ui__.nodes = nodes;
    this.validateState();
    this.propagateIOSchemas();
  }
  setConnections(connections) {
    this.state.config.connections = connections;
    this.propagateIOSchemas();
  }
  propagateIOSchemas() {
    var nodesMap = {};
    this.state.__ui__.nodes.forEach(function(n) {
      nodesMap[n.name] = n;
    });
    this.CanvasFactory
        .orderConnections(
          angular.copy(this.state.config.connections),
          this.state.artifact.name,
          this.state.__ui__.nodes
        )
        .forEach( connection => {
          let from = connection.from;
          let to = connection.to;

          if (!nodesMap[from].outputSchema) {
            return;
          }
          nodesMap[to].inputSchema = nodesMap[from].outputSchema;
          if (!nodesMap[to].outputSchema) {
            nodesMap[to].outputSchema = nodesMap[to].inputSchema;
          }
        });
  }
  addNode(node) {
    this.state.__ui__.nodes.push(node);
  }
  getNodes() {
    return this.getState().__ui__.nodes;
  }
  getNode(nodeId) {
    let nodes = this.state.__ui__.nodes;
    let match = nodes.filter( node => node.name === nodeId);
    if (match.length) {
      match = match[0];
    } else {
      match = null;
    }
    return angular.copy(match);
  }
  editNodeProperties(nodeId, nodeConfig) {
    let nodes = this.state.__ui__.nodes;
    let match = nodes.filter( node => node.name === nodeId);
    if (match.length) {
      match = match[0];
      angular.forEach(nodeConfig, (pValue, pName) => match[pName] = pValue);
      this.validateState();
    }
  }
  getSchedule() {
    return this.getState().config.schedule;
  }
  setSchedule(schedule) {
    this.state.config.schedule = schedule;
  }

  validateState(isShowConsoleMessage) {
    let isStateValid = true;
    let name = this.getName();
    let errorFactory = this.NonStorePipelineErrorFactory;
    let daglevelvalidation = [
      errorFactory.hasOnlyOneSource,
      errorFactory.hasAtLeastOneSink
    ];
    let nodes = this.state.__ui__.nodes;
    nodes.forEach( node => { node.errorCount = 0;});
    let ERROR_MESSAGES = this.GLOBALS.en.hydrator.studio.error;
    let showConsoleMessage = (errObj) => {
      if (isShowConsoleMessage) {
        this.ConsoleActionsFactory.addMessage(errObj);
      }
    };
    let setErrorWarningFlagOnNode = (node) => {
      if (node.error) {
        delete node.warning;
      } else {
        node.warning = true;
      }
      if (isShowConsoleMessage) {
        node.error = true;
        delete node.warning;
      }
    };

    daglevelvalidation.forEach( validationFn => {
      validationFn(nodes, (err, node) => {
        let content;
        if (err) {
          isStateValid = false;
          if (node) {
            node.errorCount += 1;
            setErrorWarningFlagOnNode(node);
            content = node.plugin.label + ' ' + ERROR_MESSAGES[err];
          } else {
            content = ERROR_MESSAGES[err];
          }
          showConsoleMessage({
            type: 'error',
            content: content
          });
        }
      });
    });
    errorFactory.hasValidName(name, (err) => {
      if (err) {
        isStateValid = false;
        showConsoleMessage({
          type: 'error',
          content: ERROR_MESSAGES[err]
        });
      }
    });
    errorFactory.isRequiredFieldsFilled(nodes, (err, node, unFilledRequiredFields) => {
      if (err) {
        isStateValid = false;
        node.errorCount += unFilledRequiredFields;
        setErrorWarningFlagOnNode(node);
        showConsoleMessage({
          type: 'error',
          content: node.plugin.label + ' ' + ERROR_MESSAGES[err]
        });
      }
    });

    let uniqueNodeNames = {};
    errorFactory.isUniqueNodeNames(nodes, (err, node) => {
      if (err) {
        isStateValid = false;
        node.errorCount += 1;
        setErrorWarningFlagOnNode(node);
        uniqueNodeNames[node.plugin.label] = node.plugin.label + ' ' + ERROR_MESSAGES[err];
      }
    });
    if (Object.keys(uniqueNodeNames).length) {
      angular.forEach(uniqueNodeNames, err => {
        showConsoleMessage({
          type: 'error',
          content: err
        });
      });
    }
    return isStateValid;
  }
  getInstance() {
    return this.getState().config.instance;
  }
  setInstance(instance) {
    this.state.config.instance = instance;
  }

  setComments(comments) {
    this.state.config.comments = comments;
  }
  getComments() {
    return this.getState().config.comments;
  }

  saveAsDraft(config) {
    this.state.__ui__.isEditing = false;
    this.mySettings.get('adapterDrafts')
      .then(res => {
        res = res || {isMigrated: true};
        if (!angular.isObject(this.myHelpers.objectQuery(res, this.$stateParams.namespace))) {
          res[this.$stateParams.namespace] = {};
        }
        res[this.$stateParams.namespace][config.name] = config;
        return this.mySettings.set('adapterDrafts', res);
      })
      .then(
        () => {
          this.ConsoleActionsFactory.addMessage({
            type: 'success',
            content: `Draft ${config.name} saved successfully.`
          });
        },
        err => {
          this.state.__ui__.isEditing = true;
          this.ConsoleActionsFactory.addMessage({
            type: 'error',
            content: err
          });
        }
      );
  }
}

ConfigStore.$inject = ['ConfigDispatcher', 'CanvasFactory', 'GLOBALS', 'mySettings', 'ConsoleActionsFactory', '$stateParams', 'myHelpers', 'NonStorePipelineErrorFactory'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigStore', ConfigStore);
