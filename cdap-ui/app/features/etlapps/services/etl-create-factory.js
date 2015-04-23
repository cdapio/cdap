angular.module(PKG.name + '.feature.etlapps')
  .factory('ETLAppsApiFactory', function(MyDataSource, $filter, $state, $alert, $timeout, mySettings) {
    var filterFilter = $filter('filter');
    function ETLAppsApiFactory(scope) {
      this.scope = scope;
      this.dataSrc = new MyDataSource(scope);
    }

    ETLAppsApiFactory.prototype.fetchSources = function(etlType) {
      this.dataSrc.request({
        _cdapPath: '/templates/etl.' + etlType + '/sources'
      })
        .then(function(res) {
          this.scope.etlSources = res;
        }.bind(this));
    }

    ETLAppsApiFactory.prototype.fetchSinks = function(etlType) {
      this.dataSrc.request({
        _cdapPath: '/templates/etl.'+ etlType + '/sinks'
      })
        .then(function(res) {
          this.scope.etlSinks = res;
        }.bind(this));
    }

    ETLAppsApiFactory.prototype.fetchTransforms = function(etlType) {
      this.dataSrc.request({
        _cdapPath: '/templates/etl.' + etlType + '/transforms'
      })
        .then(function(res) {
          this.scope.etlTransforms = res;
        }.bind(this));
    }

    ETLAppsApiFactory.prototype.fetchSourceProperties = function(etlSource) {
      if (!etlSource) return;
      this.dataSrc.request({
        _cdapPath: '/templates/etl.' + this.scope.metadata.type + '/sources/' + etlSource
      })
        .then(function(res) {
          this.scope.source.name = res.name;
          var obj = {};
          angular.forEach(res.properties, function(property) {
            obj[property.name] = '';
          });
          this.scope.source.properties = obj;
          this.scope.loadingEtlSourceProps = false;
        }.bind(this));
      this.scope.loadingEtlSourceProps = etlSource || false;
    }

    ETLAppsApiFactory.prototype.fetchSinkProperties = function(etlSink){
      if (!etlSink) return;
      this.dataSrc.request({
        _cdapPath: '/templates/etl.' + this.scope.metadata.type + '/sinks/' + etlSink
      })
        .then(function(res) {
          this.scope.sink.name = res.name;
          var obj = {};
          angular.forEach(res.properties, function(property) {
            obj[property.name] = '';
          });
          this.scope.sink.properties = obj;
          this.scope.loadingEtlSinkProps = false;
        }.bind(this));
      this.scope.loadingEtlSinkProps = etlSink || false;
    }

    ETLAppsApiFactory.prototype.fetchTransformProperties = function(etlTransform, index) {
      if(!etlTransform) return;
      this.dataSrc.request({
        _cdapPath: '/templates/etl.' + this.scope.metadata.type + '/transforms/' + etlTransform
      })
        .then(function(res) {
          var obj = {};
          angular.forEach(res.properties, function(property) {
            obj[property.name] = '';
          });
          index = (typeof index === 'undefined' ? this.scope.transforms.length - 1: index);
          this.scope.transforms[index].properties = obj;
        }.bind(this));
    }

    ETLAppsApiFactory.prototype.save = function (data) {
      this.dataSrc.request({
        method: 'PUT',
        _cdapPath: '/namespaces/'
                    + $state.params.namespace +
                    '/adapters/' +
                    this.scope.metadata.name,
        body: data
      })
        .then(function(res) {
          delete this.scope.etlDrafts[this.scope.metadata.name];
          mySettings.set('etldrafts', this.scope.etlDrafts)
            .then(function() {
              $timeout(function() {
                $state.go('^.list', $state.params, {reload: true});
              });
              $alert({
                type: 'success',
                content: 'ETL Template created successfully!'
              });
            })
        }.bind(this));
    }
    return ETLAppsApiFactory;

  });
