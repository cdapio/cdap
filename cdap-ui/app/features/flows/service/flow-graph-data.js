angular.module(PKG.name + '.feature.flows')
  .service('FlowDiagramData', function(MyDataSource, $state, $q) {
    this.data = {};
    // Need to figure out a way to destroy the cache when scope gets destroyed.
    var dataSrc = new MyDataSource();

    this.fetchData = function(appId, flowId) {
      var registeredFlows = Object.keys(this.data);
      if (registeredFlows.indexOf(flowId) > -1) {
        return $q.when(this.data[flowId]);
      }

      var defer = $q.defer();

      dataSrc.request({
        _cdapNsPath: '/apps/' + appId + '/flows/' + flowId
      })
        .then(function(res) {
          var nodes = [];
          angular.forEach(res.connections, function(conn) {
            if (conn.sourceType === 'STREAM') {
              nodes.push({
                type: conn.sourceType,
                name: conn.sourceName
              });
            }
          });

          angular.forEach(res.flowlets, function (val, key) {
            val.type = 'FLOWLET';
            val.name = key;
            nodes.push(val);
          });

          this.data[flowId] = {
            nodes: nodes,
            edges: res.connections,
            metrics: {}
          };
          defer.resolve(this.data[flowId]);
        }.bind(this));
      return defer.promise;
    }

  });
