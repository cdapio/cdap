angular.module(PKG.name + '.feature.adapters')
  .controller('CanvasController', function (myAdapterApi, MyPlumbService, $bootstrapModal, $state, $scope, $alert, myHelpers, CanvasFactory, MyPlumbFactory, AdapterErrorFactory) {

    this.nodes = [];

    if ($scope.AdapterCreateController.data) {
      setNodesAndConnectionsFromDraft.call(this, $scope.AdapterCreateController.data);
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
              function sucess(adapter) {
                $alert({
                  type: 'success',
                  content: adapter + ' successfully published.'
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
                icon: MyPlumbFactory.getIcon(plugin.name)
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

    function setNodesAndConnectionsFromDraft(data) {
      var ui = data.ui;
      var config = data.config;
      var nodes;
      // Purely for feeding my-plumb to draw the diagram
      // if I already have the nodes and connections
      if (ui && ui.nodes) {
        nodes = ui.nodes;
        Object.keys(nodes).forEach(function(node) {
          this.nodes.push(nodes[node]);
        }.bind(this));
      } else {
        this.nodes = CanvasFactory.getNodes(config);
      }
      this.nodes.forEach(function(node) {
        MyPlumbService.addNodes(node, node.type);
      });

      if (ui && ui.connections) {
        MyPlumbService.connections = ui.connections;
      } else {
        MyPlumbService.connections = CanvasFactory.getConnectionsBasedOnNodes(this.nodes);
      }

      var config = CanvasFactory.extractMetadataFromDraft(data.config, data);

      MyPlumbService.metadata.name = config.name;
      MyPlumbService.metadata.description = config.description;
      MyPlumbService.metadata.template = config.template;
    }

  });
