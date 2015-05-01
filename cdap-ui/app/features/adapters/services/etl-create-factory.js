angular.module(PKG.name + '.feature.adapters')
  .factory('AdapterApiFactory', function(MyDataSource, $filter, $state, $alert, $timeout, mySettings) {
    var filterFilter = $filter('filter');
    function AdapterApiFactory(scope) {
      this.scope = scope;
      this.dataSrc = new MyDataSource(scope);
    }

    AdapterApiFactory.prototype.fetchSources = function(adapterType) {
      this.dataSrc.request({
        _cdapPath: '/templates/' + adapterType + '/extensions/source'
      })
        .then(function(res) {
          this.scope.defaultSources = res;
        }.bind(this));
    }

    AdapterApiFactory.prototype.fetchSinks = function(adapterType) {
      this.dataSrc.request({
        _cdapPath: '/templates/'+ adapterType + '/extensions/sink'
      })
        .then(function(res) {
          this.scope.defaultSinks = res;
        }.bind(this));
    }

    AdapterApiFactory.prototype.fetchTransforms = function(adapterType) {
      this.dataSrc.request({
        _cdapPath: '/templates/' + adapterType + '/extensions/transform'
      })
        .then(function(res) {
          this.scope.defaultTransforms = res;
        }.bind(this));
    }

    AdapterApiFactory.prototype.fetchUIConfigurations = function(templateId, pluginId) {
      this.dataSrc.config({
        templateid: templateId,
        pluginid: pluginId
      })
       .then(function(res) {
         console.log(res);
         this.scope.templatePluginConfig = res;
       }.bind(this));
    }

    AdapterApiFactory.prototype.fetchSourceProperties = function(source) {
      if (!source) return;
      this.dataSrc.request({
        _cdapPath: '/templates/' + this.scope.metadata.type + '/extensions/source/plugins/' + source
      })
        .then(function(res) {
          var s = res[0];
          this.scope.source.name = s.name;
          this.scope.source._backendProperties = s.properties;
          var obj = {};
          angular.forEach(s.properties, function(property) {
            obj[property.name] = '';
          });
          this.scope.source.properties = obj;
        }.bind(this));
    }

    AdapterApiFactory.prototype.fetchSinkProperties = function(sink){
      if (!sink) return;
      this.dataSrc.request({
        _cdapPath: '/templates/' + this.scope.metadata.type + '/extensions/sink/plugins/' + sink
      })
        .then(function(res) {
          var s = res[0];
          this.scope.sink.name = s.name;
          this.scope.sink._backendProperties = s.properties;
          var obj = {};
          angular.forEach(s.properties, function(property) {
            obj[property.name] = '';
          });
          this.scope.sink.properties = obj;
        }.bind(this));
    }

    AdapterApiFactory.prototype.fetchTransformProperties = function(transform, index) {
      if(!transform) return;
      this.dataSrc.request({
        _cdapPath: '/templates/' + this.scope.metadata.type + '/extensions/transform/plugins/' + transform
      })
        .then(function(res) {
          var t = res[0];
          var obj = {};
          angular.forEach(t.properties, function(property) {
            obj[property.name] = '';
          });
          index = (typeof index === 'undefined' ? this.scope.transforms.length - 1: index);
          this.scope.transforms[index].properties = obj;
          this.scope.transforms[index]._backendProperties = t.properties;
        }.bind(this));
    }

    AdapterApiFactory.prototype.save = function (data) {
      this.dataSrc.request({
        method: 'PUT',
        _cdapPath: '/namespaces/'
                    + $state.params.namespace +
                    '/adapters/' +
                    this.scope.metadata.name,
        body: data
      })
        .then(function(res) {
          delete this.scope.adapterDrafts[this.scope.metadata.name];
          return mySettings.set('adapterdrafts', this.scope.adapterDrafts)
        }.bind(this))
        .then(function() {
          this.scope.isSaved = true;
          $timeout(function() {
            $state.go('^.list', $state.params, {reload: true});
          });
          $alert({
            type: 'success',
            content: 'Adapter Template created successfully!'
          });
        }.bind(this))
    }
    return AdapterApiFactory;

  });
