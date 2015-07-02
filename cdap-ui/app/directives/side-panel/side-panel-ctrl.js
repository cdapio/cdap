angular.module(PKG.name + '.commons')
  .controller('MySidePanel', function ($scope, myAdapterApi, MyPlumbService) {
    this.items = $scope.panelGroups;
    this.openPlugins = function (type) {
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

    this.addPlugin = function(config, type) {
      MyPlumbService.updateConfig(config, type);
    };

    this.initializePlugins = function(type, plugins) {
      this.plugins = plugins.map(function(plugin) {
        return angular.extend({type: type}, plugin);
      });
    };

    this.hidePluginContainer = function() {
      this.showPlugin = false;
    };

    this.showPluginContainer = function() {
      this.showPlugin = true;
    };

  });
