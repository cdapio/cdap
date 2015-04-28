angular.module(PKG.name + '.feature.adapters')
  .service('PluginConfigFactory', function(MyDataSource, $q) {
    this.plugins = {};

    this.fetch = function(scope, templateid, pluginid) {

      if (this.plugins[templateid+pluginid]) {
        return $q.when(this.plugins[templateid+pluginid]);
      }
      var dataSrc = new MyDataSource(scope);
      var defer = $q.defer();


      dataSrc.config({
        templateid: templateid, //'etlRealtime',
        pluginid: pluginid //'TwitterSource'
      })
        .then(function(res) {
          this.plugins[templateid+pluginid] = res;
          defer.resolve(this.plugins[templateid+pluginid]);
        }.bind(this));
      return defer.promise;
    };

  });
