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

    this.groups = [
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

    this.panel= {
      items: []
    };

    this.onGroupClicked = function(group) {
      var prom;
      switch(group.name) {
        case 'source':
          prom = myAdapterApi.fetchSources({
            adapterType: 'ETLRealtime'
          })
            .$promise;
          break;
        case 'transform':
          prom = myAdapterApi.fetchTransforms({
            adapterType: 'ETLRealtime'
          })
            .$promise;
          break;
        case 'sink':
          prom = myAdapterApi.fetchSinks({
            adapterType: 'ETLRealtime'
          })
            .$promise;
          break;
      }
      prom.then(function(res) {
        this.panel.items = [];
        res.forEach(function(plugin) {
          this.panel.items.push(
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

    this.onPanelItemClicked = function(event, item) {
      event.stopPropagation();
      MyPlumbService.updateConfig(item, item.type);
    };
  });
