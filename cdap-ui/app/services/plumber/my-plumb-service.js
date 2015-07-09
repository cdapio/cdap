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
  .service('MyPlumbService', function(myAdapterApi, $q, $bootstrapModal) {
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

    this.updateConnection = function(connections) {
      this.connections = connections.map(function(conn) {
        return {
          source: conn.sourceId,
          target: conn.targetId
        };
      });
    };

    this.updateNodes = function(conf, type) {
      var config = {
        id: conf.id,
        name: conf.name,
        icon: conf.icon,
        description: conf.description,
        type: conf.type
      }
      this.nodes[config.id] = config;
      this.notifyListeners(config, type);
    };

    this.editPluginProperties = function (scope, pluginId, pluginType) {
      var plugin = this.nodes[pluginId];
      var prom = $q.defer();
      var propertiesApiMap = {
        'source': myAdapterApi.fetchSourceProperties,
        'sink': myAdapterApi.fetchSinkProperties,
        'transform': myAdapterApi.fetchTransformProperties
      };
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
            controller: ['$scope', 'AdapterModel', 'type', function ($scope, AdapterModel, type){
              $scope.plugin = AdapterModel;
              $scope.type = type;
              $scope.isDisabled = false;
            }],
            size: 'lg',
            resolve: {
              AdapterModel: function () {
                return plugin;
              },
              type: function () {
                return 'ETLBatch';
              }
            }
          });
        })
    };
  });
