angular.module(PKG.name + '.feature.foo')
  .controller('PlumbController', function(myAdapterApi, MyPlumbService) {
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
        icon: 'fa fa-play'
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
        icon: 'fa fa-download'
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

    this.plugins= {
      items: []
    };

    this.onPluginTypesClicked = function(group) {
      var prom;
      switch(group.name) {
        case 'source':
          prom = myAdapterApi.fetchSources({ adapterType: 'ETLRealtime' }).$promise;
          break;
        case 'transform':
          prom = myAdapterApi.fetchTransforms({ adapterType: 'ETLRealtime' }).$promise;
          break;
        case 'sink':
          prom = myAdapterApi.fetchSinks({ adapterType: 'ETLRealtime' }).$promise;
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
      }.bind(this))
    };

    this.onCanvasOperationsClicked = function(group) {
    }

    this.onPluginItemClicked = function(event, item) {
      event.stopPropagation();
      MyPlumbService.addNodes(item, item.type);
    };
  });
