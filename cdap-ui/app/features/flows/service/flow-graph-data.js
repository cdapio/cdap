angular.module(PKG.name + '.feature.flows')
  .factory('FlowDiagramData', function(MyDataSource, $state, $q) {
    var flowdata = {};
    var dataSrc = new MyDataSource(),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    flowdata.fetchData = function() {
      var defer = $q.defer();

      dataSrc.request({
        _cdapNsPath: basePath
      })
        .then(function(res) {
          var nodes = [],
              metrics = {}, data;
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

          data = {
            nodes: nodes,
            edges: res.connections,
            metrics: {}
          };
          defer.resolve(data);
        });
      return defer.promise;
    }
    return flowdata;

  });
