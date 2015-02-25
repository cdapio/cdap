angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailRunStatusController', function($state, $scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    var metricsPath = '/metrics/query?metric=system.process.events.processed' +
                          '&context=ns.' +
                          $state.params.namespace +
                          '.app.' + $state.params.appId +
                          '.flow.' + $state.params.programId +
                          '.run.' + $state.params.runId +
                          '.flowlet.';

    $scope.data = {};
    $scope.status = null;
    $scope.duration = null;
    $scope.startTime = null;
    dataSrc.request({
      _cdapNsPath: basePath
    })
      .then(function(res) {
        var nodes = [],
            metrics = {};
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

        $scope.data = {
          nodes: nodes,
          edges: res.connections,
          metrics: {}
        };

        // Requesting Metrics data
        angular.forEach(nodes, function (node) {
          dataSrc.request({
            _cdapPath: metricsPath + node.name,
            method: 'POST'
          })
            .then(function (data) {

              $scope.data.metrics[node.name] = data.series[0] ? data.series[0].data[0].value : 0;

            });

        });
      });



    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        angular.forEach(res, function(run) {
          if (run.runid === $state.params.runId) {
            $scope.status = run.status;
            $scope.duration = (run.end? run.end - run.start: Date.now() - run.start);
            $scope.startTime = run.start*1000;
          }
        });
      });


  });
