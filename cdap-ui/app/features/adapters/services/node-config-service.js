angular.module(PKG.name + '.feature.adapters')
  .service('MyNodeConfigService', function() {
    this.pluginChangeListeners = [];
    this.setPlugin = function(plugin) {
      this.notifyListeners(plugin);
    };

    this.notifyListeners = function (plugin) {

      this.pluginChangeListeners.forEach(function(callback) {
        callback(plugin);
      });
      // $q.all(promises)
      //   .then(function(values) {
      //     if (values.indexOf(false) === -1) {
      //       this.plugin = plugin;
      //     }
      //   }.bind(this));
    };

    this.registerPluginCallback = function(callback) {
      this.pluginChangeListeners.push(callback);
    };

  });
