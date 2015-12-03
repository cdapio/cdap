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

/*
  Service that maintains the list of nodes and connections
  MyAppDAGService is responsible for communicating between side panel and plumb directives

  Adding Nodes from Side-Panel:
    1. When the user clicks/drag-n-drops a plugin this service is notified of it.
    2. The service then updates all the listeners who have registered for this change
    3. The plumb directive will eventually get this notification and will draw a node.

  Making connections in UI in the plumb-directive:
    1. When the user makes a connection in the UI this service gets notified of that connection.
    2. (In the future) if someone is interested then they can register for this event.

  Editing Properties in canvas-ctrl:
    1. When the user wants to edit the properties of a plugin this service gets the notification
    2. The plugin ID will be sent. Now the service should fetch the list of properties for the plugin.
    3. Create a map of properties and add it to the plugin (identified by passed in plugin ID)
        in the list of nodes.
    4. Now we have an object to bind to the UI (properties modal).
    5. Any edits user make on the modal should automatically get saved.

  Saving/Publishing an adapter from canvas-ctrl:
    1. When we want to publish an adapter this service will be notified
    2. It will go through the list of connections and forms the config based on the node information
        we have from the list of nodes (along with its properties).
    3. Performs UI validation
    4. Creates a new Object for saving and saves it to backend.
    5. On error should show the errors in a container at the bottom of the page.
    6. Nice-to-have - when the user clicks on the error he should be highlighted with what is the problem.


*/
angular.module(PKG.name + '.services')
  .service('MyAppDAGService', function(myAdapterApi, $q, $bootstrapModal, $state, $filter, mySettings, AdapterErrorFactory, IMPLICIT_SCHEMA, myHelpers, PluginConfigFactory, ModalConfirm, EventPipe, CanvasFactory, $rootScope, GLOBALS, MyNodeConfigService, MyConsoleTabService) {

    var countSink = 0,
        countSource = 0,
        countTransform = 0;

    this.resetToDefaults = function(isImport) {
      var callbacks = angular.copy(this.callbacks);
      var errorCallbacks = angular.copy(this.errorCallbacks);
      var resetCallbacks = angular.copy(this.resetCallbacks);
      var editPropertiesCallback = angular.copy(this.editPropertiesCallback);

      this.callbacks = [];
      this.errorCallbacks = [];
      this.resetCallbacks = [];
      this.editPropertiesCallback = [];
      this.nodes = {};
      this.connections = [];
      var name = this.metadata && this.metadata.name;
      this.metadata = {
        name: '',
        description: '',
        template: {
          type: GLOBALS.etlBatch,
          instance: 1,
          schedule: {
            cron: '* * * * *'
          }
        }
      };

      this.isConfigTouched = false;

      countSink = 0;
      countSource = 0;
      countTransform = 0;
      // This is needed when we import a config from an already created draft.
      // In that case we already have a name & callbacks registered and we don't want to lose it.
      // So resetting everything except name.
      // Resetting template should be fine as it is going to be the same.
      isImport = (isImport === true? true: false);
      if (isImport) {
        this.callbacks = callbacks;
        this.resetCallbacks = resetCallbacks;
        this.errorCallbacks = errorCallbacks;
        this.metadata.name = name;
        this.editPropertiesCallback = editPropertiesCallback;
      }
    };

    this.resetToDefaults();

    this.registerResetCallBack = function(callback) {
      this.resetCallbacks.push(callback);
    };
    this.notifyResetListners = function () {
      this.resetCallbacks.forEach(function(callback) {
        callback();
      });
    };

    this.registerCallBack = function (callback) {
      this.callbacks.push(callback);
    };

    this.notifyListeners = function(conf, type) {
      this.callbacks.forEach(function(callback) {
        callback(conf, type);
      });
    };

    this.errorCallback = function (callback) {
      this.errorCallbacks.push(callback);
    };

    this.notifyError = function (errorObj) {
      this.errorCallbacks.forEach(function(callback) {
        callback(errorObj);
      });
    };

    this.registerEditPropertiesCallback = function(callback) {
      this.editPropertiesCallback.push(callback);
    };

    this.notifyEditPropertiesCallback = function(plugin) {
      this.editPropertiesCallback.forEach(function(callback) {
        callback(plugin);
      });
    };

    this.addConnection = function(connection) {
      this.connections.push({
        source: connection.sourceId,
        target: connection.targetId
      });
    };

    function addConnectionsInOrder(node, finalConnections, originalConnections) {
      if (node.visited) {
        return finalConnections;
      }

      node.visited = true;
      finalConnections.push(node);
      var nextConnection = originalConnections.filter(function(conn) {
        if (node.target === conn.source) {
          return conn;
        }
      });
      if (nextConnection.length) {
        return addConnectionsInOrder(nextConnection[0], finalConnections, originalConnections);
      }
    }

    function findTransformThatIsSource(originalConnections) {
      var transformAsSource = {};
      function isSource (c) {
        if (c.target === connection.source) {
          return c;
        }
      }
      for (var i =0; i<originalConnections.length; i++) {
        var connection = originalConnections[i];
        var isSoureATarget = originalConnections.filter(isSource);
        if (!isSoureATarget.length) {
          transformAsSource = connection;
          break;
        }
      }
      return transformAsSource;
    }

    function orderConnections(connections, originalConnections) {
      if (!originalConnections.length) {
        return originalConnections;
      }
      var finalConnections = [];
      var parallelConnections = [];
      var source = connections.filter(function(conn) {
        if (this.nodes[conn.source].type === GLOBALS.pluginTypes[this.metadata.template.type].source) {
          return conn;
        }
      }.bind(this));

      if (source.length) {
        addConnectionsInOrder(source[0], finalConnections, originalConnections);
        if (finalConnections.length < originalConnections.length) {
          originalConnections.forEach(function(oConn) {
            if ($filter('filter')(finalConnections, oConn).length === 0) {
              parallelConnections.push(oConn);
            }
          });
          finalConnections = finalConnections.concat(parallelConnections);
        }
      } else {
        source = findTransformThatIsSource(originalConnections);
        addConnectionsInOrder(source, finalConnections, originalConnections);
      }
      return finalConnections;
    }

    this.formatSchema = function (node) {
      var isStreamSource = node.name === 'Stream';
      var schema;
      var input;
      var jsonSchema;

      if (isStreamSource) {
        if (node.properties.format === 'clf') {
          jsonSchema = IMPLICIT_SCHEMA.clf;
        } else if (node.properties.format === 'syslog') {
          jsonSchema = IMPLICIT_SCHEMA.syslog;
        } else {
          jsonSchema = node.outputSchema;
        }
      } else {
        jsonSchema = node.outputSchema;
      }

      try {
        input = JSON.parse(jsonSchema);
      } catch (e) {
        input = null;
      }

      if (isStreamSource) {
        // Must be in this order!!
        if (!input) {
          input = {
            fields: [{ name: 'body', type: 'string' }]
          };
        }

        input.fields.unshift({
          name: 'headers',
          type: {
            type: 'map',
            keys: 'string',
            values: 'string'
          }
        });

        input.fields.unshift({
          name: 'ts',
          type: 'long'
        });

      }

      schema = input ? input.fields : null;
      angular.forEach(schema, function (field) {
        if (angular.isArray(field.type)) {
          field.type = field.type[0];
          field.nullable = true;
        } else {
          field.nullable = false;
        }
      });

      return schema;
    };


    this.setConnections = function(connections) {
      this.isConfigTouched = true;
      this.connections = [];
      var localConnections = [];
      connections.forEach(function(con) {
        localConnections.push({
          source: con.sourceId,
          target: con.targetId
        });
      });
      localConnections = orderConnections.call(this, angular.copy(localConnections), angular.copy(localConnections));
      this.connections = localConnections;

    };

    this.addNodes = function(conf, type, inCreationMode) {
      var defer = $q.defer();
      this.isConfigTouched = true;

      var config = {
        id: conf.id,
        name: conf.name,
        label: conf.label || conf.pluginTemplate || conf.name,
        icon: conf.icon,
        style: conf.style || '',
        description: conf.description,
        outputSchema: conf.outputSchema || null,
        pluginTemplate: conf.pluginTemplate || null,
        errorDatasetName: conf.errorDatasetName || '',
        validationFields: conf.validationFields || null,
        lock: conf.lock || null,
        properties: conf.properties || {},
        _backendProperties: conf._backendProperties,
        type: conf.type
      };

      angular.extend(config, styleNode.call(this, type, inCreationMode));

      this.nodes[config.id] = config;

      if (!conf._backendProperties) {
        fetchBackendProperties
          .call(this, this.nodes[config.id])
          .then(function() {
            this.nodes[config.id].properties = this.nodes[config.id].properties || {};
            angular.forEach(this.nodes[config.id]._backendProperties, function(value, key) {
              this.nodes[config.id].properties[key] = this.nodes[config.id].properties[key] || '';
            }.bind(this));
            defer.resolve(this.nodes[config.id]);
            MyNodeConfigService.notifyPluginSaveListeners(config.id);
          }.bind(this));

      } else if(Object.keys(conf._backendProperties).length !== Object.keys(conf.properties).length) {
        angular.forEach(conf._backendProperties, function(value, key) {
          config.properties[key] = config.properties[key] || '';
        });
        defer.resolve(this.nodes[config.id]);
        MyNodeConfigService.notifyPluginSaveListeners(config.id);
      }

      if (inCreationMode) {
        /*
          The reason to use a promise here is to fetch the backend properties for each plugin
          before notifying listeners about the change. This helps in showing the #of required
          fields as a warning in the nodes in DAG as soon as they are added to Canvas.
        */
        defer
          .promise
          .then(function() {
            this.notifyListeners(config, type);
          }.bind(this));
      }
      return defer.promise;
    };

    this.removeNode = function (nodeId) {
      this.isConfigTouched = true;
      var type = this.nodes[nodeId].type;
      var artifactTypeExtension = GLOBALS.pluginTypes[this.metadata.template.type];
      switch (type) {
        case artifactTypeExtension.source:
          countSource--;
          break;
        case 'transform':
          countTransform--;
          break;
        case artifactTypeExtension.sink:
          countSink--;
          break;
      }

      delete this.nodes[nodeId];
    };

    this.setIsDisabled = function(isDisabled) {
      this.isDisabled = isDisabled;
    };

    function styleNode(type, inCreationMode) {
      var config = {};
      // FIXME: Utterly irrelavant place to set style. This needs to be moved ASAP.
      var artifactType = GLOBALS.pluginTypes[this.metadata.template.type];
      var offsetLeft = 0;
      var offsetTop = 0;
      var initial = 0;

      if (type === artifactType.source) {
        initial = 10;

        offsetLeft = countSource * 2;
        offsetTop = countSource * 70;

        countSource++;

      } else if (type === 'transform') {
        initial = 30;

        offsetLeft = countTransform * 2;
        offsetTop = countTransform * 70;

        countTransform++;

      } else if (type === artifactType.sink) {
        initial = 50;

        offsetLeft = countSink * 2;
        offsetTop = countSink * 70;

        countSink++;
      }

      var left = initial + offsetLeft;
      var top = 100 + offsetTop;

      if (inCreationMode) {
        config.style = {
          left: left + 'vw',
          top: top + 'px'
        };
      }
      return config;
    }

    function fetchBackendProperties(plugin, scope) {
      var defer = $q.defer();
      var sourceType = GLOBALS.pluginTypes[this.metadata.template.type].source,
          sinkType = GLOBALS.pluginTypes[this.metadata.template.type].sink;

      var propertiesApiMap = {
        'transform': myAdapterApi.fetchTransformProperties
      };
      propertiesApiMap[sourceType] = myAdapterApi.fetchSourceProperties;
      propertiesApiMap[sinkType] = myAdapterApi.fetchSinkProperties;

      // This needs to pass on a scope always. Right now there is no cleanup
      // happening
      var params = {
        namespace: $state.params.namespace,
        adapterType: this.metadata.template.type,
        version: $rootScope.cdapVersion,
        extensionType: plugin.type,
        pluginName: plugin.name
      };
      if (scope) {
        params.scope = scope;
      }

      return propertiesApiMap[plugin.type](params)
        .$promise
        .then(function(res) {

          var pluginProperties = (res.length? res[0].properties: {});
          if (res.length && (!plugin.description || (plugin.description && !plugin.description.length))) {
            plugin.description = res[0].description;
          }
          plugin._backendProperties = pluginProperties;
          defer.resolve(plugin);
          return defer.promise;
        }, function error () {
          MyConsoleTabService.addMessage({
            type: 'error',
            content: GLOBALS.en.hydrator.studio.pluginDoesNotExist + params.pluginName
          });
          plugin._backendProperties = false;
          plugin.requiredFieldCount = '!';
        });
    }

    this.editPluginProperties = function (scope, pluginId) {
      this.notifyEditPropertiesCallback(this.nodes[pluginId]);
    };

    // Used for UI alone. Has _backendProperties and ids to plugins for
    // construction and validation of DAGs in UI.
    this.getConfig = function() {
      var config = {
        artifact: {
          name: this.metadata.name,
          scope: 'SYSTEM',
          version: $rootScope.cdapVersion
        },
        source: {
          properties: {}
        },
        sinks: [],
        transforms: []
      };
      var artifactTypeExtension = GLOBALS.pluginTypes[this.metadata.template.type];
      var nodes = angular.copy(this.nodes);

      function addPluginToConfig(plugin, id) {

        var pluginConfig =  {
          // Solely adding id and _backendProperties for validation.
          // Should be removed while saving it to backend.
          id: plugin.id,
          name: plugin.name,
          label: plugin.label,
          properties: plugin.properties,
          _backendProperties: plugin._backendProperties
        };

        if (plugin.type === artifactTypeExtension.source) {
          config['source'] = pluginConfig;
        } else if (plugin.type === 'transform') {
          if (plugin.errorDatasetName && plugin.errorDatasetName.length > 0) {
            pluginConfig.errorDatasetName = plugin.errorDatasetName;
          }
          if (plugin.validationFields) {
            pluginConfig.validationFields = plugin.validationFields;
          }

          config['transforms'].push(pluginConfig);
        } else if (plugin.type === artifactTypeExtension.sink) {
          config['sinks'].push(pluginConfig);
        }

        delete nodes[id];
      }

      this.connections.forEach(function (connection) {
        if (nodes[connection.source]) {
          addPluginToConfig(nodes[connection.source], connection.source);
        }
        if (nodes[connection.target]) {
          addPluginToConfig(nodes[connection.target], connection.target);
        }
      });
      pruneNonBackEndProperties(config);
      return config;
    };

    function pruneNonBackEndProperties(config) {
      function propertiesIterator(properties, backendProperties) {
        angular.forEach(properties, function(value, key) {
          // If its a required field don't remove it.
          var isRequiredField = backendProperties[key] && backendProperties[key].required;
          var isErrorDatasetName = !backendProperties[key] && key !== 'errorDatasetName';
          var isPropertyEmptyOrNull = properties[key] === '' || properties[key] === null;

          if (!isRequiredField && (isErrorDatasetName || isPropertyEmptyOrNull)) {
            delete properties[key];
          }
          // FIXME: Remove this once https://issues.cask.co/browse/CDAP-3614 is fixed.
          if (angular.isDefined(properties[key]) && !angular.isString(properties[key])) {
            properties[key] = properties[key].toString();
          }

        });
        return properties;
      }
      if (myHelpers.objectQuery(config, 'source', 'properties') &&
          Object.keys(config.source.properties).length > 0) {
        config.source.properties = propertiesIterator(config.source.properties, config.source._backendProperties);
      }

      config.sinks.forEach(function(sink) {
        if (myHelpers.objectQuery(sink, 'properties') &&
            Object.keys(sink.properties).length > 0) {
          sink.properties = propertiesIterator(sink.properties, sink._backendProperties);
        }
      });

      config.transforms.forEach(function(transform) {
        if (myHelpers.objectQuery(transform, 'properties') &&
            Object.keys(transform.properties).length > 0) {
          transform.properties = propertiesIterator(transform.properties, transform._backendProperties);
        }
      });
    }

    function pruneProperties(config) {

      pruneNonBackEndProperties(config);

      if (config.source && (config.source.id || config.source._backendProperties)) {
        delete config.source._backendProperties;
        delete config.source.id;
      }

      config.sinks.forEach(function(sink) {
        delete sink._backendProperties;
        delete sink.id;
      });

      config.transforms.forEach(function(t) {
        delete t._backendProperties;
        delete t.id;
      });
    }

    // Used to save to backend. Has no fluff. Just real stuff that is needed.
    this.getConfigForBackend = function () {
      var config = this.getConfig();
      pruneProperties(config);
      var data = {
        artifact: {
          name: this.metadata.template.type,
          scope: 'SYSTEM',
          version: $rootScope.cdapVersion
        },
        config: {
          source: config.source,
          sinks: config.sinks,
          transforms: config.transforms
        },
        description: this.metadata.description
      };
      if (this.metadata.template.type === GLOBALS.etlRealtime) {
        data.config.instances = this.metadata.template.instance;
      } else if (this.metadata.template.type === GLOBALS.etlBatch) {
        // default value should be * * * * *
        data.config.schedule = this.metadata.template.schedule.cron;
      }
      return data;
    };
    this.save = function() {
      var defer = $q.defer();

      if (MyNodeConfigService.getIsPluginBeingEdited()) {
        // This should have been a popup that we show for un-saved changes while switching the node.
        // Couldn't do it here because we cannot set it to another plugin. Hence the console message.
        // If we are able to fuse 4 hydrogen atoms and things turn out good, we will have auto-correct
        // in the next realease and we should be able to remove a majority of
        // communication happening with save and reset in node configuration.
        this.notifyError({
          canvas: [
            GLOBALS.en.hydrator.studio.unsavedPluginMessage1 +
            MyNodeConfigService.plugin.label +
            GLOBALS.en.hydrator.studio.unsavedPluginMessage2
          ]
        });
        defer.reject();
        return defer.promise;
      }
      this.isConfigTouched = false;
      var config = this.getConfig();
      var errors = AdapterErrorFactory.isModelValid(this.nodes, this.connections, this.metadata, config);

      if (!angular.isObject(errors)) {
        EventPipe.emit('showLoadingIcon', 'Publishing Pipeline to CDAP');
        var data = this.getConfigForBackend();
        myAdapterApi.save(
          {
            namespace: $state.params.namespace,
            adapter: this.metadata.name
          },
          data
        )
          .$promise
          .then(
            function success() {
              mySettings.get('adapterDrafts')
               .then(function(res) {
                 var adapterName = this.metadata.name;
                 if (angular.isObject(res)) {
                   delete res[adapterName];
                   mySettings.set('adapterDrafts', res);
                 }
                 defer.resolve(adapterName);
                 this.resetToDefaults();
                 EventPipe.emit('hideLoadingIcon.immediate');
               }.bind(this));
            }.bind(this),
            function error(err) {
              defer.reject({
                messages: err
              });
              this.notifyError({
                canvas: [err.data]
              });
              EventPipe.emit('hideLoadingIcon.immediate');
            }.bind(this)
          );
      } else {
        this.notifyError(errors);
        defer.reject(errors);
      }
      return defer.promise;
    };

    this.saveAsDraft = function() {
      this.isConfigTouched = false;
      var defer = $q.defer();
      var config = this.getConfigForBackend();
      var error = {};
      AdapterErrorFactory.hasNameAndTemplateType(null, null, this.metadata, null, error);

      if (Object.keys(error).length) {
        this.notifyError(error);
        defer.reject(true);
        return defer.promise;
      }
      config.ui = {
        nodes: this.nodes,
        connections: this.connections
      };
      return mySettings.get('adapterDrafts')
        .then(function(res) {
          if (!angular.isObject(res)) {
            res = {};
          }
          res[this.metadata.name] = config;
          return mySettings.set('adapterDrafts', res);
        }.bind(this));
    };

    this.setNodesAndConnectionsFromDraft = function(data) {
      var ui = data.ui;
      var config = data.config;
      var nodes = [];
      var config1 = CanvasFactory.extractMetadataFromDraft(data);

      if (config1.name) {
        this.metadata.name = config1.name;
      }
      this.metadata.description = config1.description;
      this.metadata.template = config1.template;

      if (ui && ui.nodes) {
        angular.forEach(ui.nodes, function(value) {
          nodes.push(value);
        }.bind(this));
      } else {
        nodes = CanvasFactory.getNodes(config, this.metadata.template.type);
      }
      nodes.forEach(function(node) {
        this.addNodes(node, node.type);
      }.bind(this));

      if (ui && ui.connections) {
        this.connections = ui.connections;
      } else {
        this.connections = CanvasFactory.getConnectionsBasedOnNodes(nodes, this.metadata.template.type);
      }
      this.notifyError({});
      this.notifyResetListners();
    };

    this.onImportSuccess = function(result) {
      EventPipe.emit('popovers.reset');
      var config = JSON.stringify(result);
      this.resetToDefaults(true);
      this.setNodesAndConnectionsFromDraft(result);
      if (config.name) {
        this.metadata.name = config.name;
      }
    };

  });
