angular.module(PKG.name + '.feature.adapters')
  .controller('CanvasController', function (myAdapterApi, MyPlumbService, $bootstrapModal, $state, $scope, $alert, myHelpers, AdapterErrorFactory) {
    function getIcon(plugin) {
      var iconMap = {
        'script': 'fa-code',
        'twitter': 'fa-twitter',
        'cube': 'fa-cubes',
        'data': 'fa-database',
        'database': 'fa-database',
        'table': 'fa-table',
        'kafka': 'icon-kafka',
        'stream': 'icon-plugin-stream',
        'avro': 'icon-avro',
        'jms': 'icon-jms'
      };

      var pluginName = plugin.toLowerCase();
      var icon = iconMap[pluginName] ? iconMap[pluginName]: 'fa-plug';
      return icon;
    }

    this.nodes = [];

    if ($scope.AdapterCreateController.data) {
      var data = $scope.AdapterCreateController.data;
      var ui = data.ui;
      var config = data.config;
      // Purely for feeding my-plumb to draw the diagram
      // if I already have the nodes and connections
      if (ui && ui.nodes) {
        var nodes = ui.nodes;
        Object.keys(nodes).forEach(function(node) {
          var nodeObj = nodes[node];
          this.nodes.push(nodeObj);
          MyPlumbService.addNodes(angular.copy(nodeObj), nodeObj.type);
        }.bind(this));
      } else {
        this.nodes = getNodes(config);
        this.nodes.forEach(function(node) {
          MyPlumbService.addNodes(angular.copy(node), node.type);
        });
      }

      if (ui && ui.connections) {
        MyPlumbService.connections = ui.connections;
      }

      // Too many ORs. Should be removed when all drafts eventually are
      // resaved in the new format. This is temporary
      MyPlumbService.metadata.name = myHelpers.objectQuery(config, 'metadata', 'name')
      || myHelpers.objectQuery(data, 'name');

      MyPlumbService.metadata.description = myHelpers.objectQuery(config, 'metadata', 'description')
      || myHelpers.objectQuery(data, 'description');

      var template = myHelpers.objectQuery(config, 'metadata', 'type')
      || myHelpers.objectQuery(data, 'template');

      if (template === 'ETLBatch') {
        MyPlumbService.metadata.template.schedule.cron = myHelpers.objectQuery(config, 'schedule', 'cron')
        || myHelpers.objectQuery(config, 'schedule');
      } else if (template === 'ETLRealtime') {
        MyPlumbService.metadata.template.instance = myHelpers.objectQuery(config, 'instance')
        || myHelpers.objectQuery(config, 'metadata', 'template', 'instance');
      }
    }
    function getNodes(config) {
      var nodes = [];
      var i =0;
      nodes.push({
        id: config.source.name + (++i),
        name: config.source.name,
        type: 'source',
        properties: config.source.properties
      });
      config.transforms.forEach(function(transform) {
        nodes.push({
          id: transform.name + (++i),
          name: transform.name,
          type: 'transform',
          properties: transform.properties
        });
      });
      nodes.push({
        id: config.sink.name + (++i),
        name: config.sink.name,
        type: 'sink',
        properties: config.sink.properties
      });
      return nodes;
    }

    this.pluginTypes = [
      {
        name: 'source',
        icon: 'icon-ETLsources'
      },
      {
        name: 'transform',
        icon: 'icon-ETLtransforms'
      },
      {
        name: 'sink',
        icon: 'icon-ETLsinks'
      }
    ];

    this.canvasOperations = [
      {
        name: 'Publish',
        icon: 'fa fa-cloud-upload'
      },
      {
        name: 'Save Draft',
        icon: 'fa fa-save'
      },
      {
        name: 'Config',
        icon: 'fa fa-eye'
      },
      {
        name: 'Settings',
        icon: 'fa fa-cogs'
      }
    ];

    this.onCanvasOperationsClicked = function(group) {
      var config;
      switch(group.name) {
        case 'Config':
          config = angular.copy(MyPlumbService.getConfigForBackend());
          $bootstrapModal.open({
            templateUrl: '/assets/features/adapters/templates/create/viewconfig.html',
            size: 'lg',
            keyboard: true,
            controller: ['$scope', 'config', function($scope, config) {
              $scope.config = JSON.stringify(config);
            }],
            resolve: {
              config: function() {
                return config;
              }
            }
          });
          break;
        case 'Publish':
          MyPlumbService
            .save()
            .then(
              function sucess() {
                $alert({
                  type: 'success',
                  content: MyPlumbService.metadata.name + ' successfully published.'
                });
                $state.go('adapters.list');
              },
              function error(errorObj) {
                console.error('ERROR!: ', errorObj);
                // AdapterErrorFactory.processError(errorObj);
              }.bind(this)
            );
          break;
        case 'Save Draft':
          MyPlumbService
            .saveAsDraft()
            .then(
              function success() {
                $alert({
                  type: 'success',
                  content: MyPlumbService.metadata.name + ' successfully saved as draft.'
                });
                $state.go('adapters.list');
              },
              function error() {
                $alert({
                  type: 'danger',
                  content: 'Problem saving ' + MyPlumbService.metadata.name + ' as draft'
                });
              }
            )
      }
    };

    this.plugins= {
      items: []
    };

    this.onPluginTypesClicked = function(group) {
      var prom;
      switch(group.name) {
        case 'source':
          prom = myAdapterApi.fetchSources({ adapterType: 'ETLBatch' }).$promise;
          break;
        case 'transform':
          prom = myAdapterApi.fetchTransforms({ adapterType: 'ETLBatch' }).$promise;
          break;
        case 'sink':
          prom = myAdapterApi.fetchSinks({ adapterType: 'ETLBatch' }).$promise;
          break;
      }
      prom.then(function(res) {
        this.plugins.items = [];
        res.forEach(function(plugin) {
          this.plugins.items.push(
            angular.extend(
              {
                type: group.name,
                icon: getIcon(plugin.name)
              },
              plugin
            )
          );
        }.bind(this));
      }.bind(this));
    };

    this.onPluginItemClicked = function(event, item) {
      if (item.type === 'source' && this.pluginTypes[0].error) {
        delete this.pluginTypes[0].error;
      } else if (item.type === 'sink' && this.pluginTypes[2].error) {
        delete this.pluginTypes[2].error;
      }

      // TODO: Better UUID?
      var id = item.name + '-' + item.type + '-' + Date.now();
      event.stopPropagation();
      var config = {
        id: id,
        name: item.name,
        icon: item.icon,
        description: item.description,
        type: item.type
      };
      MyPlumbService.addNodes(config, config.type);
    };

    function errorNotification(errors) {
      angular.forEach(this.pluginTypes, function (type) {
        if (errors[type.name]) {
          type.error = errors[type.name];
        }
      });
    }

    MyPlumbService.errorCallback(errorNotification.bind(this));

  });
