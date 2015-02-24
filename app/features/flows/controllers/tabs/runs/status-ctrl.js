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
          edges: res.connections
        };
      });


    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        angular.forEach(res, function(run) {
          if (run.runid === $state.params.runId) {
            $scope.status = run.status;
            $scope.duration = (run.end ? run.end - run.start : (Date.now()/1000) - run.start);
            $scope.startTime = run.start*1000;
          }
        });
      });

  });
