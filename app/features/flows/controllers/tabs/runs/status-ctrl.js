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
        res.connections.forEach(function(conn) {
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
        var match = res.filter(function(run) {
          return run.runid === $state.params.runId;
        })[0];
        console.log("run status: ", match.status);
        $scope.status = match.status;
        $scope.duration = (match.end? match.end - match.start: Date.now() - match.start);
        $scope.startTime = match.start;
      });

  });
