angular.module(PKG.name + '.feature.foo')
  .controller('PlumbController', function(myAdapterApi) {
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
    this.panelConfig = {};
    this.panelGroups = [];
    myAdapterApi.fetchSources({
      adapterType: 'ETLRealtime'
    })
      .$promise
      .then(addToConfig.bind(this, 'source'));
    myAdapterApi.fetchSinks({
      adapterType: 'ETLRealtime'
    })
      .$promise
      .then(addToConfig.bind(this, 'sink'));
    myAdapterApi.fetchTransforms({
      adapterType: 'ETLRealtime'
    })
      .$promise
      .then(addToConfig.bind(this, 'transform'));

    function addToConfig(type, res) {
      this.panelGroups.push(type)
      this.panelConfig[type] = res.map(function(extension) {
        return {
          name: extension.name,
          description: extension.description,
          icon: getIcon(extension.name.toLowerCase())
        };
      });
    }
  });
