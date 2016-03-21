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
  constructor(ConfigDispatcher, CanvasFactory, GLOBALS, mySettings, ConsoleActionsFactory, $stateParams, myHelpers, NonStorePipelineErrorFactory, HydratorService, $q, PluginConfigFactory, uuid, $state){
    this.state = {};
    this.mySettings = mySettings;
    this.ConsoleActionsFactory = ConsoleActionsFactory;
    this.CanvasFactory = CanvasFactory;
    this.GLOBALS = GLOBALS;
    this.$stateParams = $stateParams;
    this.myHelpers = myHelpers;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;
    this.HydratorService = HydratorService;
    this.$q = $q;
    this.PluginConfigFactory = PluginConfigFactory;
    this.uuid = uuid;
    this.$state = $state;

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
    this.configDispatcher.register('onSchemaPropagationDownStream', this.propagateIOSchemas.bind(this));
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
        draftId: null
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
  getDraftId() {
    return this.state.__ui__.draftId;
  }
  setDraftId(draftId) {
    this.state.__ui__.draftId = draftId;
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
      nodesMap[n.name] = angular.copy(n);
    });
    // Strip out schema property of the plugin if format is clf or syslog
    let stripFormatSchemas = (formatProp, outputSchemaProp, properties) => {
      if (!formatProp || !outputSchemaProp) {
        return properties;
      }
      if (['clf', 'syslog'].indexOf(properties[formatProp]) !== -1) {
        delete properties[outputSchemaProp];
      }
      return properties;
    };

    let addPluginToConfig = (node, id) => {
      if (node.outputSchemaProperty) {
        try {
          let outputSchema = JSON.parse(node.outputSchema);
          if (angular.isArray(outputSchema.fields)) {
            outputSchema.fields = outputSchema.fields.filter( field => !field.readonly);
          }
          node.plugin.properties[node.outputSchemaProperty] = JSON.stringify(outputSchema);
        } catch(e) {}
      }
      node.plugin.properties = stripFormatSchemas(node.watchProperty, node.outputSchemaProperty, angular.copy(node.plugin.properties));
      var pluginConfig =  {
        // Solely adding id and _backendProperties for validation.
        // Should be removed while saving it to backend.
        name: node.plugin.name,
        label: node.plugin.label,
        artifact: node.plugin.artifact,
        properties: node.plugin.properties ,
        _backendProperties: node._backendProperties
      };

      if (node.type === artifactTypeExtension.source) {
        config['source'] = {
          name: node.plugin.label,
          plugin: pluginConfig,
          outputSchema: node.outputSchema,
          inputSchema: node.inputSchema
        };
      } else if (node.type === 'transform') {
        if (node.plugin.validationFields) {
          pluginConfig.validationFields = node.plugin.validationFields;
        }
        pluginConfig = {
          name: node.plugin.label,
          plugin: pluginConfig,
          outputSchema: node.outputSchema,
          inputSchema: node.inputSchema
        };

        if (node.errorDatasetName && node.errorDatasetName.length > 0) {
          pluginConfig.errorDatasetName = node.errorDatasetName;
        }

        config['transforms'].push(pluginConfig);
      } else if (node.type === artifactTypeExtension.sink) {
        pluginConfig = {
          name: node.plugin.label,
          plugin: pluginConfig,
          outputSchema: node.outputSchema,
          inputSchema: node.inputSchema
        };
        config['sinks'].push(pluginConfig);
      }
      delete nodesMap[id];
    };

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
      delete node.error;
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
    let listOfPromises = [];
    // Prepopulate nodes with backend properties;
    // This will be used for cases where we import/use a predefined app and when we render the entire
    // dag we need to show #of errors in each node (badge on the top right corner of each node).
    let nodesWOutBackendProps = this.state.__ui__.nodes.filter(
      node => !angular.isObject(node._backendProperties)
    );
    let parseNodeConfig = (node, res) => {
      let nodeConfig = this.PluginConfigFactory.generateNodeConfig(node._backendProperties, res);
      node.implicitSchema = nodeConfig.outputSchema.implicitSchema;
      node.outputSchemaProperty = nodeConfig.outputSchema.outputSchemaProperty;
      if (angular.isArray(node.outputSchemaProperty)) {
        node.outputSchemaProperty = node.outputSchemaProperty[0];
        node.watchProperty = nodeConfig.outputSchema.schemaProperties['property-watch'];
      }
      if (node.outputSchemaProperty) {
        node.outputSchema = node.plugin.properties[node.outputSchemaProperty];
      }
      if (nodeConfig.outputSchema.implicitSchema) {
        let keys = Object.keys(nodeConfig.outputSchema.implicitSchema);
        let formattedSchema = [];
        angular.forEach(keys, (key) => {
          formattedSchema.push({
            name: key,
            type: nodeConfig.outputSchema.implicitSchema[key]
          });
        });
        node.outputSchema = JSON.stringify({ fields: formattedSchema });
      }
    };
    if (nodesWOutBackendProps) {
      nodesWOutBackendProps.forEach( n => {
        listOfPromises.push(this.HydratorService.fetchBackendProperties(n, this.getAppType()));
      });

    } else {
      listOfPromises.push(this.$q.when(true));
    }
    this.$q.all(listOfPromises)
      .then(
        () => {
          if(!this.validateState()) {
            this.emitChange();
          }
          // Once the backend properties are fetched for all nodes, fetch their config jsons.
          // This will be used for schema propagation where we import/use a predefined app/open a published pipeline
          // the user should directly click on the last node and see what is the incoming schema
          // without having to open the subsequent nodes.
          nodesWOutBackendProps.forEach( n => {
            // This could happen when the user doesn't provide an artifact information for a plugin & deploys it
            // using CLI or REST and opens up in UI and clones it. Without this check it will throw a JS error.
            if (!n.plugin.artifact) { return; }
            this.PluginConfigFactory.fetchWidgetJson(
              n.plugin.artifact.name,
              n.plugin.artifact.version,
              n.plugin.artifact.scope,
              `widgets.${n.plugin.name}-${n.type}`
            ).then(parseNodeConfig.bind(null, n));
          });
        },
        (err) => console.log('ERROR fetching backend properties for nodes', err)
      );

    let setDefaultOutputSchemaForNodes = (node) => {
      var pluginName = node.plugin.name;
      var pluginToSchemaMap = {
        'Stream': [
          {
            readonly: true,
            name: 'ts',
            type: 'long'
          },
          {
            readonly: true,
            name: 'headers',
            type: {
              type: 'map',
              keys: 'string',
              values: 'string'
            }
          }
        ]
      };
      if (pluginToSchemaMap[pluginName]){
        if (!node.outputSchema) {
          node.outputSchema = {
            fields: [{ name: 'body', type: 'string' }]
          };
          node.outputSchema = JSON.stringify({ fields: pluginToSchemaMap[pluginName].concat(node.outputSchema.fields)});
        }
      }
    };
    this.state.__ui__.nodes.forEach(node=> setDefaultOutputSchemaForNodes(node));
  }
  setConnections(connections) {
    this.state.config.connections = connections;
  }
  // This is for the user to forcefully propagate the output schema of a node
  // down the stream to all its connections.
  // Its a simple BFS down the graph to propagate the schema. Right now it doesn't catch cycles.
  // The assumption there are no cycles in the dag we create.
  propagateIOSchemas(pluginId) {
    let adjacencyMap = {},
        nodesMap = {},
        outputSchema,
        schema,
        connections = this.state.config.connections;
    this.state.__ui__.nodes.forEach( node => nodesMap[node.name] = node );

    connections.forEach( conn => {
      if (Array.isArray(adjacencyMap[conn.from])) {
        adjacencyMap[conn.from].push(conn.to);
      } else {
        adjacencyMap[conn.from] = [conn.to];
      }
    });

    let traverseMap = (node, outputSchema) => {
      if (!node) {
        return;
      }
      // If we encounter an implicit schema down the stream while propagation stop there and return.
      if (node.isImplicitSchema) {
        return;
      }
      node.forEach( n => {
        nodesMap[n].outputSchema = outputSchema;
        nodesMap[n].inputSchema = outputSchema;
        if (nodesMap[n].outputSchemaProperty) {
          nodesMap[n].plugin.properties[nodesMap[n].outputSchemaProperty] = outputSchema;
        }
        traverseMap(adjacencyMap[n], outputSchema);
      });
    };
    outputSchema = nodesMap[pluginId].outputSchema;
    try {
      schema = JSON.parse(outputSchema);
      schema.fields = schema.fields.map(field => {
        delete field.readonly;
        return field;
      });
      outputSchema = JSON.stringify(schema);
    } catch (e) {}
    traverseMap(adjacencyMap[pluginId], outputSchema);
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
  getSourceNodes(nodeId) {
    let nodesMap = {};
    this.state.__ui__.nodes.forEach( node => nodesMap[node.name] = node );
    return this.state.config.connections.filter( conn => conn.to === nodeId ).map( matchedConnection => nodesMap[matchedConnection.from] );
  }
  editNodeProperties(nodeId, nodeConfig) {
    let nodes = this.state.__ui__.nodes;
    let match = nodes.filter( node => node.name === nodeId);
    if (match.length) {
      match = match[0];
      angular.forEach(nodeConfig, (pValue, pName) => match[pName] = pValue);
      if (!this.validateState()) {
        this.emitChange();
      }
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
    let connections = angular.copy(this.state.config.connections);
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
    errorFactory.allNodesConnected(nodes, connections, (errorNode) => {
      if (errorNode) {
        isStateValid = false;
        showConsoleMessage({
          type: 'error',
          content: errorNode.plugin.name + ' ' + this.GLOBALS.en.hydrator.studio.error['MISSING-CONNECTION']
        });
      }
    });

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

  saveAsDraft() {
    this.ConsoleActionsFactory.resetMessages();
    if(!this.getDraftId()) {
      this.setDraftId(this.uuid.v4());
      this.$stateParams.draftId = this.getDraftId();
      this.$state.go('hydrator.create.studio', this.$stateParams, {notify: false});
    }
    let config = this.getState();
    // This is not to fall in the scenario where when the user saves a draft with a node selected.
    // Next time they come to the draft and we still have the node selected but the bottom panel not updated.
    config.__ui__.nodes = config.__ui__.nodes.map( node => {
      delete node.selected;
      delete node.error;
      return node;
    });
    let checkForDuplicateDrafts = (config, draftsMap = {}) => {
      return Object.keys(draftsMap).filter(
        draft => {
          return draftsMap[draft].name === config.name &&
                 config.__ui__.draftId !== draftsMap[draft].__ui__.draftId;
        }
      ).length > 0;
    };
    let saveDraft = (config, draftsMap = {}) => {
      draftsMap[config.__ui__.draftId] = config;
      return draftsMap;
    };
    this.mySettings.get('hydratorDrafts', true)
      .then( (res = {isMigrated: true}) => {
        let draftsMap = res[this.$stateParams.namespace];
        if(!checkForDuplicateDrafts(config, draftsMap)) {
          res[this.$stateParams.namespace] = saveDraft(config, draftsMap);
        } else {
          throw 'A Draft with the same name already exist. Plesae rename your draft';
        }
        return this.mySettings.set('hydratorDrafts', res);
      })
      .then(
        () => {
          this.ConsoleActionsFactory.addMessage({
            type: 'success',
            content: `Draft ${config.name} saved successfully.`
          });
        },
        err => {
          this.ConsoleActionsFactory.addMessage({
            type: 'error',
            content: err
          });
        }
      );
  }
}

ConfigStore.$inject = ['ConfigDispatcher', 'CanvasFactory', 'GLOBALS', 'mySettings', 'ConsoleActionsFactory', '$stateParams', 'myHelpers', 'NonStorePipelineErrorFactory', 'HydratorService', '$q', 'PluginConfigFactory', 'uuid', '$state'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigStore', ConfigStore);
