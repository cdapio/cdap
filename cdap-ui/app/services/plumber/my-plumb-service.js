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

  Editing Properties in canvas-ctrl: -- NOT DONE
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
  .service('MyPlumbService', function(myAdapterApi, $q, $bootstrapModal, $filter) {
    this.callbacks = [];
    this.nodes = {};
    this.connections = [];

    this.registerCallBack = function (callback) {
      this.callbacks.push(callback);
    };

    this.notifyListeners = function(conf, type) {
      this.callbacks.forEach(function(callback) {
        callback(conf, type);
      });
    };

    this.addConnection = function(connection) {
      this.connections.push({
        source: connection.sourceId,
        target: connection.targetId
      });
    };

    this.setConnections = function(connections) {
      this.connections = [];
      var connection = angular.copy(connections);
      connection.forEach(this.addConnection.bind(this));
    };

    this.addNodes = function(conf, type) {
      var config = {
        id: conf.id,
        name: conf.name,
        icon: conf.icon,
        description: conf.description,
        type: conf.type
      };
      this.nodes[config.id] = config;
      this.notifyListeners(config, type);
    };

    this.removeNode = function (nodeId) {
      delete this.nodes[nodeId];
    };

    this.editPluginProperties = function (scope, pluginId, pluginType) {
      var propertiesApiMap = {
        'source': myAdapterApi.fetchSourceProperties,
        'sink': myAdapterApi.fetchSinkProperties,
        'transform': myAdapterApi.fetchTransformProperties
      };

      var sourceConn = $filter('filter')(this.connections, { target: pluginId });
      var sourceSchema = null;

      if (sourceConn.length) {
        var source = sourceConn.length ? this.nodes[sourceConn[0].source] : null;

        sourceSchema = source.outputSchema;

      }

      var plugin = this.nodes[pluginId];
      var prom = $q.defer();

      var params = {
        scope: scope,
        adapterType: 'ETLBatch'
      };
      params[pluginType] = plugin.name;

      propertiesApiMap[pluginType](params)
        .$promise
        .then(function(res) {
          var pluginProperties = (res.length? res[0].properties: {});
          plugin._backendProperties = pluginProperties;
          prom.resolve(plugin);
          return prom.promise;
        })
        .then(function(plugin) {
          $bootstrapModal.open({
            animation: false,
            templateUrl: '/assets/features/adapters/templates/tabs/runs/tabs/properties/properties.html',
            controller: ['$scope', 'AdapterModel', 'type', 'inputSchema', function ($scope, AdapterModel, type, inputSchema){
              $scope.plugin = AdapterModel;
              $scope.type = type;
              $scope.isDisabled = false;

              var input;
              try {
                input = JSON.parse(inputSchema);
              } catch (e) {
                input = null;
              }
              $scope.inputSchema = input ? input.fields : null;

              if (!$scope.plugin.outputSchema && inputSchema) {
                $scope.plugin.outputSchema = angular.copy(inputSchema) || null;
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
                $scope.hideInput = true;
              }

              if (AdapterModel.type === 'sink') {
                $scope.isSink = true;
              }

            }],
            size: 'lg',
            resolve: {
              AdapterModel: function () {
                return plugin;
              },
              type: function () {
                return 'ETLBatch';
              },
              inputSchema: function () {
                return sourceSchema;
              }
            }
          });
        });
    };

    this.getConfig = function() {
      var config = {
        source: {
          properties: {}
        },
        sink: {
          properties: {}
        },
        transforms: []
      };
      var errors = [];
      var conn;
      var i;
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
      return config;
    };

    this.save = function() {
      return this.isModelValid();
    };
    this.isModelValid = function() {
      var validationRules = [
        hasExactlyOneSourceAndSink,
        hasNameAndTemplateType,
        checkForRequiredField,
        checkForUnconnectedNodes,
        checkForParallelDAGs
      ];
      var errors = [];
      validationRules.forEach(function(rule) {
        var errorObj = rule.call(this);
        if (angular.isArray(errorObj)) {
          errors = errors.concat(errorObj);
        } else if (angular.isObject(errorObj)) {
          errors.push(errorObj);
        }
      }.bind(this));
      return errors.length? errors: true;
    };

    function hasExactlyOneSourceAndSink() {
      var source =0, sink =0;
      var errObj = true;
      this.connections.forEach(function(conn) {
        // Source cannot be target just like sink cannot be source in a connection in DAG.
        source += (this.nodes[conn.source].type === 'source'? 1: 0);
        sink += (this.nodes[conn.target].type === 'sink'? 1: 0);
      }.bind(this));
      if ( source + sink !== 2) {
        errObj = {};
        errObj.type = 'nodes';
        errObj.message = 'Adapter should have exactly one source and one sink';
      }
      return errObj;
    }

    function hasNameAndTemplateType() {

    }
    function checkForRequiredField() {
      var errors = true;
      var config = this.getConfig();
      function addToErrors(type, message) {
        if (!angular.isArray(errors)) {
          errors = [];
        }
        var obj = {};
        obj.type = type;
        obj.message = message;
        errors.push(obj);
      }
      if(!isValidPlugin(config.source)) {
        addToErrors('nodes', 'Adapter\'s source is missing required fields');
      }
      if (!isValidPlugin(config.sink)) {
        addToErrors('nodes', 'Adapter\'s sink is missing required fields');
      }
      config.transforms.forEach(function(transform) {
        if (!isValidPlugin(transform)) {
          addToErrors('nodes', 'Adapter\'s transforms is missing required fields');
        }
      });
      return errors;
    }
    function isValidPlugin(plugin) {
      var i;
      var keys = Object.keys(plugin.properties);
      if (!keys.length) {
        plugin.valid = false;
        return plugin.valid;
      }
      plugin.valid = true;
      for (i=0; i< keys.length; i++) {
        var property = plugin.properties[keys[i]];
        if (plugin._backendProperties[keys[i]].required && (!property || property === '')) {
          plugin.valid = false;
          break;
        }
      }
      return plugin.valid;
    }
    function checkForUnconnectedNodes() {

    }
    function checkForParallelDAGs() {

    }
  });
