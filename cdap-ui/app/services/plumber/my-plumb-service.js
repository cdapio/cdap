/*
  Service that maintains the list of nodes and connections
  MyPlumbService is responsible for communicating between side panel and plumb directives

  Adding Nodes from Side-Panel: -- DONE
    1. When the user clicks/drag-n-drops a plugin this service is notified of it.
    2. The service then updates all the listeners who have registered for this change
    3. The plumb directive will eventually get this notification and will draw a node.

  Making connections in UI in the plumb-directive: -- DONE
    1. When the user makes a connection in the UI this service gets notified of that connection.
    2. (In the future) if someone is interested then they can register for this event.

  Editing Properties in canvas-ctrl: -- DONE
    1. When the user wants to edit the properties of a plugin this service gets the notification
    2. The plugin ID will be sent. Now the service should fetch the list of properties for the plugin.
    3. Create a map of properties and add it to the plugin (identified by passed in plugin ID)
        in the list of nodes.
    4. Now we have an object to bind to the UI (properties modal).
    5. Any edits user make on the modal should automatically get saved.

  Saving/Publishing an adapter from canvas-ctrl: -- NOT DONE
    1. When we want to publish an adapter this service will be notified
    2. It will go through the list of connections and forms the config based on the node information
        we have from the list of nodes (along with its properties).
    3. Performs UI validation
    4. Creates a new Object for saving and saves it to backend.
    5. On error should show the errors in a container at the bottom of the page.
    6. Nice-to-have - when the user clicks on the error he should be highlighted with what is the problem.


*/
angular.module(PKG.name + '.services')
  .service('MyPlumbService', function(myAdapterApi, $q, $bootstrapModal, $state, $filter, mySettings, AdapterErrorFactory, IMPLICIT_SCHEMA, myHelpers, PluginConfigFactory, ModalConfirm, EventPipe) {

    var countSink = 0,
        countSource = 0,
        countTransform = 0;

    this.resetToDefaults = function(isImport) {
      var callbacks = angular.copy(this.callbacks);
      var errorCallbacks = angular.copy(this.errorCallbacks);
      var resetCallbacks = angular.copy(this.resetCallbacks);

      this.callbacks = [];
      this.errorCallbacks = [];
      this.resetCallbacks = [];
      this.nodes = {};
      this.connections = [];
      var name = this.metadata && this.metadata.name;
      this.metadata = {
        name: '',
        description: '',
        template: {
          type: 'ETLBatch',
          instance: 1,
          schedule: {
            cron: '* * * * *'
          }
        }
      };

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
      for (var i =0; i<originalConnections.length; i++) {
        var connection = originalConnections[i];
        var isSoureATarget = originalConnections.filter(function (c) {
          if (c.target === connection.source) {
            return c;
          }
        });
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
        if (this.nodes[conn.source].type === 'source') {
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
        var source = findTransformThatIsSource(originalConnections);
        addConnectionsInOrder(source, finalConnections, originalConnections);
      }
      return finalConnections;
    }


    this.setConnections = function(connections) {
      this.connections = [];
      var localConnections = [];
      connections.forEach(function(con) {
        localConnections.push({
          source: con.sourceId,
          target: con.targetId
        });
      });
      var localConnections = orderConnections.call(this, angular.copy(localConnections), angular.copy(localConnections));
      this.connections = localConnections;
    };

    this.addNodes = function(conf, type, inCreationMode) {
      var config = {
        id: conf.id,
        name: conf.name,
        icon: conf.icon,
        style: conf.style || '',
        description: conf.description,
        outputSchema: conf.outputSchema || '',
        properties: conf.properties || {},
        _backendProperties: conf._backendProperties,
        type: conf.type
      };
      var offsetLeft = 0;
      var offsetTop = 0;
      var initial = 0;

      if (type === 'source') {
        initial = 30;

        offsetLeft = countSource * 2;
        offsetTop = countSource * 70;

        countSource++;

      } else if (type === 'transform') {
        initial = 50;

        offsetLeft = countTransform * 2;
        offsetTop = countTransform * 70;

        countTransform++;

      } else if (type === 'sink') {
        initial = 70;

        offsetLeft = countSink * 2;
        offsetTop = countSink * 70;

        countSink++;
      }

      var left = initial + offsetLeft;
      var top = 250 + offsetTop;

      if (inCreationMode) {
        config.style = {
          left: left + 'vw',
          top: top + 'px'
        };
      }

      this.nodes[config.id] = config;

      PluginConfigFactory.fetch(
        null,
        this.metadata.template.type,
        config.name
      ).then(function (res) {
        if (res.implicit) {
          var schema = res.implicit.schema;
          var keys = Object.keys(schema);

          var formattedSchema = [];
          angular.forEach(keys, function (key) {
            formattedSchema.push({
              name: key,
              type: schema[key]
            });
          });

          var obj = { fields: formattedSchema };
          this.nodes[config.id].outputSchema = JSON.stringify(obj);
        }
      }.bind(this));

      if (!conf._backendProperties) {
        fetchBackendProperties
          .call(this, this.nodes[config.id])
          .then(function() {
            this.nodes[config.id].properties = this.nodes[config.id].properties || {};
            angular.forEach(this.nodes[config.id]._backendProperties, function(value, key) {
              this.nodes[config.id].properties[key] = this.nodes[config.id].properties[key] || '';
            }.bind(this));
          }.bind(this));

      } else if(Object.keys(conf._backendProperties).length !== Object.keys(conf.properties).length) {
        angular.forEach(conf._backendProperties, function(value, key) {
          config.properties[key] = config.properties[key] || '';
        });
      }
      if (inCreationMode) {
        this.notifyListeners(config, type);
      }
    };

    this.removeNode = function (nodeId) {
      var type = this.nodes[nodeId].type;

      switch (type) {
        case 'source':
          countSource--;
          break;
        case 'transform':
          countTransform--;
          break;
        case 'sink':
          countSink--;
          break;
      }

      delete this.nodes[nodeId];
    };

    this.setIsDisabled = function(isDisabled) {
      this.isDisabled = isDisabled;
    };

    function fetchBackendProperties(plugin, scope) {
      var defer = $q.defer();

      var propertiesApiMap = {
        'source': myAdapterApi.fetchSourceProperties,
        'sink': myAdapterApi.fetchSinkProperties,
        'transform': myAdapterApi.fetchTransformProperties
      };
      // This needs to pass on a scope always. Right now there is no cleanup
      // happening
      var params = {
        adapterType: this.metadata.template.type
      };
      if (scope) {
        params.scope = scope;
      }
      params[plugin.type] = plugin.name;

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
        });
    }

    this.editPluginProperties = function (scope, pluginId) {
      var sourceConn = $filter('filter')(this.connections, { target: pluginId });
      var sourceSchema = null;
      var isStreamSource = false;

      var clfSchema = IMPLICIT_SCHEMA.clf;

      var syslogSchema = IMPLICIT_SCHEMA.syslog;

      var source;
      if (sourceConn.length) {
        source = this.nodes[sourceConn[0].source];
        sourceSchema = source.outputSchema;

        if (source.name === 'Stream') {
          isStreamSource = true;
        }

        if (source.properties.format && source.properties.format === 'clf') {
          sourceSchema = clfSchema;
        } else if (source.properties.format && source.properties.format === 'syslog') {
          sourceSchema = syslogSchema;
        }

      } else {
        sourceSchema = this.nodes[pluginId].properties.schema || '';
      }

      var plugin = this.nodes[pluginId];
      var pluginCopy = angular.copy(plugin);

      var modalInstance;

      fetchBackendProperties.call(this, plugin, scope)
        .then(function(plugin) {
          modalInstance = $bootstrapModal.open({
            backdrop: 'static',
            templateUrl: '/assets/features/adapters/templates/tabs/runs/tabs/properties/properties.html',
            controller: ['$scope', 'AdapterModel', 'type', 'inputSchema', 'isDisabled', '$bootstrapModal', 'pluginCopy', function ($scope, AdapterModel, type, inputSchema, isDisabled, $bootstrapModal, pluginCopy){
              $scope.plugin = AdapterModel;
              $scope.type = type;
              $scope.isDisabled = isDisabled;
              var input;
              try {
                input = JSON.parse(inputSchema);
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

              $scope.inputSchema = input ? input.fields : null;
              angular.forEach($scope.inputSchema, function (field) {
                if (angular.isArray(field.type)) {
                  field.type = field.type[0];
                  field.nullable = true;
                } else {
                  field.nullable = false;
                }
              });


              if (!$scope.plugin.outputSchema && input) {
                $scope.plugin.outputSchema = angular.copy(JSON.stringify(input)) || null;
              }

              if ($scope.plugin._backendProperties.schema) {
                $scope.$watch('plugin.outputSchema', function () {
                  if (!$scope.plugin.outputSchema) {
                    if ($scope.plugin.properties && $scope.plugin.properties.schema) {
                      $scope.plugin.properties.schema = null;
                    }
                    return;
                  }

                  if (!$scope.plugin.properties) {
                    $scope.plugin.properties = {};
                  }
                  $scope.plugin.properties.schema = $scope.plugin.outputSchema;
                });
              }

              if (AdapterModel.type === 'source') {
                $scope.isSource = true;
              }

              if (AdapterModel.type === 'sink') {
                $scope.isSink = true;
              }
              if (AdapterModel.type === 'transform') {
                $scope.isTransform = true;
              }

              function closeFn() {
                $scope.$close('cancel');
              }

              ModalConfirm.confirmModalAdapter(
                $scope,
                $scope.plugin.properties,
                pluginCopy.properties,
                closeFn
              );


            }],
            size: 'lg',
            windowClass: 'adapter-modal',
            resolve: {
              AdapterModel: function () {
                return plugin;
              },
              type: function () {
                return this.metadata.template.type;
              }.bind(this),
              inputSchema: function () {
                return sourceSchema;
              },
              isDisabled: function() {
                return this.isDisabled;
              }.bind(this),
              pluginCopy: function () {
                return pluginCopy;
              },
              isStreamSource: function () {
                return isStreamSource;
              }
            }
          });


          modalInstance.result.then(function (res) {
            if (res === 'cancel') {
              this.nodes[pluginId] = angular.copy(pluginCopy);
            }
          }.bind(this));

          // destroy modal when user clicks back button or navigate out of this view
          scope.$on('$destroy', function () {
            if (modalInstance) {
              modalInstance.close();
            }
          });

        }.bind(this));


    };

    // Used for UI alone. Has _backendProperties and ids to plugins for
    // construction and validation of DAGs in UI.
    this.getConfig = function() {
      var config = {
        name: this.metadata.name,
        description: this.metadata.description,
        template: this.metadata.template.type,
        source: {
          properties: {}
        },
        sink: {
          properties: {}
        },
        transforms: []
      };
      var nodes = angular.copy(this.nodes);

      function addPluginToConfig(plugin, id) {
        if (['source', 'sink'].indexOf(plugin.type) !== -1) {
          config[plugin.type] = {
            // Solely adding id and _backendProperties for validation.
            // Should be removed while saving it to backend.
            id: plugin.id,
            name: plugin.name,
            properties: plugin.properties || {},
            _backendProperties: plugin._backendProperties
          };
        } else if (plugin.type === 'transform') {
          config.transforms.push({
            id: plugin.id,
            name: plugin.name,
            properties: plugin.properties || {},
            _backendProperties: plugin._backendProperties
          });
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
          if (!backendProperties[key] || properties[key] === '' || properties[key] === null) {
            delete properties[key];
          }
        });
        return properties;
      }
      if (myHelpers.objectQuery(config, 'source', 'properties') &&
          Object.keys(config.source.properties).length > 0) {
        config.source.properties = propertiesIterator(config.source.properties, config.source._backendProperties);
      }
      if (myHelpers.objectQuery(config, 'sink', 'properties') &&
          Object.keys(config.sink.properties).length > 0) {
        config.sink.properties = propertiesIterator(config.sink.properties, config.sink._backendProperties);
      }
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
      if (config.sink && (config.sink.id || config.sink._backendProperties)) {
        delete config.sink._backendProperties;
        delete config.sink.id;
      }
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
        template: this.metadata.template.type,
        description: this.metadata.description,
        config: {
          source: config.source,
          sink: config.sink,
          transforms: config.transforms
        }
      };
      if (this.metadata.template.type === 'ETLRealtime') {
        data.config.instances = this.metadata.template.instance;
      } else if (this.metadata.template.type === 'ETLBatch') {
        // default value should be * * * * *
        data.config.schedule = this.metadata.template.schedule.cron;
      }
      return data;
    };
    this.save = function() {
      var defer = $q.defer();
      var config = this.getConfig();
      var errors = AdapterErrorFactory.isModelValid(this.nodes, this.connections, this.metadata, config);

      if (!angular.isObject(errors)) {
        EventPipe.emit('showLoadingIcon');
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
              EventPipe.emit('hideLoadingIcon.immediate');
            }
          );
      } else {
        this.notifyError(errors);
        defer.reject(errors);
      }
      return defer.promise;
    };

    this.saveAsDraft = function() {
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

  });
