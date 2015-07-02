angular.module(PKG.name + '.commons')
  .controller('MySidePanel', function ($scope, myAdapterApi, MyPlumbService) {
    this.items = $scope.panelGroups;
    this.expanded = true;
    this.openPlugins = function (type) {
      if (this.openedPluginType === type && this.showPlugin) {
        this.showPlugin = false;
        this.openedPluginType = null;
        return;
      }

      this.openedPluginType = type;

      switch (type) {
        case 'source':
          myAdapterApi.fetchSources({
            adapterType: 'ETLRealtime'
          })
            .$promise
            .then(this.initializePlugins.bind(this, 'source'))
            .then(this.showPluginContainer.bind(this));
          break;
        case 'sink':
          myAdapterApi.fetchSinks({
            adapterType: 'ETLRealtime'
          })
            .$promise
            .then(this.initializePlugins.bind(this, 'sink'))
            .then(this.showPluginContainer.bind(this));
          break;
        case 'transform':
        myAdapterApi.fetchTransforms({
          adapterType: 'ETLRealtime'
        })
          .$promise
          .then(this.initializePlugins.bind(this, 'transform'))
          .then(this.showPluginContainer.bind(this));
      }
    };

    this.addPlugin = function(event, config, type) {
      event.stopPropagation();
      MyPlumbService.updateConfig(config, type);
    };

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

    this.initializePlugins = function(type, plugins) {
      this.plugins = plugins.map(function(plugin) {
        return angular.extend(
          {
            type: type,
            icon: getIcon(plugin.name)
          },
          plugin
        );
      });
    };

    this.hidePluginContainer = function() {
      this.showPlugin = false;
    };

    this.showPluginContainer = function() {
      this.showPlugin = true;
    };

    this.openPlugins('source');
  });
