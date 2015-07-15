angular.module(PKG.name + '.feature.adapters')
  .controller('CanvasController', function (myAdapterApi, MyPlumbService, $bootstrapModal, $state, AdapterErrorFactory) {
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

    // Purely for feeding my-plumb to draw the diagram
    // if I already have the nodes and connections
    this.nodes = [];

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
        icon: 'fa fa-save'
      },
      {
        name: 'Zoom In',
        icon: 'fa fa-search-plus'
      },
      {
        name: 'Zoom Out',
        icon: 'fa fa-search-minus'
      },
      {
        name: 'Export',
        icon: 'fa fa-eye'
      },
      {
        name: 'Import',
        icon: 'fa fa-upload'
      },
      {
        name: 'Settings',
        icon: 'fa fa-cogs'
      }
    ];

    this.onCanvasOperationsClicked = function(group) {
      var config;
      switch(group.name) {
        case 'Export':
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
                $state.go('adapters.list');
              },
              function error(errorObj) {
                console.error('ERROR!: ', errorObj);
                AdapterErrorFactory.processError(errorObj);
              }.bind(this)
            );
          break;
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


  });
