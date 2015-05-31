angular.module(PKG.name + '.feature.adapters')
  .service('PluginConfigFactory', function(MyDataSource, $q) {
    this.plugins = {};

    this.fetch = function(scope, templateid, pluginid) {
      var dataSrc = new MyDataSource(scope);
      var defer = $q.defer();


      dataSrc.config({
        templateid: templateid, //'etlRealtime',
        pluginid: pluginid //'TwitterSource'
      })
        .then(
          function success(res) {
            this.plugins[templateid+pluginid] = res;
            defer.resolve(this.plugins[templateid+pluginid]);
          }.bind(this),
          function error(err) {
            defer.reject(err);
          }
        );
      return defer.promise;
    };

  });
