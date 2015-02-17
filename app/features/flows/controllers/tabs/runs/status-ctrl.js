angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailRunStatusController', function($state, $scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    $scope.data = {};
    $scope.status = null;
    $scope.duration = null;
    $scope.startTime = null;
    dataSrc.request({
      _cdapNsPath: basePath
    })
      .then(function(res) {
        var nodes = [];
        angular.forEach(res.connections, function(conn) {
          if (conn.sourceType === 'STREAM') {
            nodes.push(conn.sourceName);
          }
        });


        nodes = nodes.concat(Object.keys(res.flowlets));
        $scope.data = {
          nodes: nodes,
          edges: res.connections
        };
      });


    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        angular.forEach(res, function(run) {
          if (run.runid === $state.params.runId) {
            $scope.status = run.status;
            $scope.duration = (run.end? run.end - run.start: Date.now() - run.start);
            $scope.startTime = run.start;
          }
        });
      });

  });
