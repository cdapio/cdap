angular.module(PKG.name + '.feature.adapters')
  .service('MyNodeConfigService', function() {
    this.pluginChangeListeners = [];
    this.setPlugin = function(plugin) {
      this.plugin = plugin;
      this.notifyListeners();
    };

    this.notifyListeners = function () {
      this.pluginChangeListeners.forEach(function(callback) {
        callback(this.plugin);
      }.bind(this));
    };

    this.registerPluginCallback = function(callback) {
      this.pluginChangeListeners.push(callback);
    };

  });
