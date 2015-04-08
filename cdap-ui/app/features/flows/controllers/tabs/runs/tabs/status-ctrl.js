angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunsDetailStatusControler', function($state, $scope, MyDataSource, myHelpers, FlowDiagramData) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    var metricFlowletPath = '/metrics/query?metric=system.process.events.processed' +
                          '&context=ns.' +
                          $state.params.namespace +
                          '.app.' + $state.params.appId +
                          '.flow.' + $state.params.programId +
                          '.run.' + $state.params.runid +
                          '.flowlet.',
        metricStreamPath = '/metrics/query?metric=system.collect.events' +
                           '&context=namespace.' +
                           $state.params.namespace +
                           '.stream.';
    console.log('state', $state.params);
    $scope.data = {};
    $scope.status = null;
    $scope.duration = null;
    $scope.startTime = null;

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then(function(data) {
        $scope.data = data;
        pollMetrics();
      });

    // This controller is NOT shared between the accordions.
    console.info("Polling on Runs");
    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        angular.forEach(res, function(run) {
          if (run.runid === $state.params.runid) {
            $scope.status = run.status;
            $scope.duration = (run.end ? run.end - run.start : (Date.now()/1000) - run.start);
            $scope.startTime = run.start*1000;
          }
        });
      });

    function pollMetrics() {
      var nodes = $scope.data.nodes;
      // Requesting Metrics data
      angular.forEach(nodes, function (node) {
        console.info("Polling on Metrics");
        dataSrc.poll({
          _cdapPath: (node.type === 'STREAM' ? metricStreamPath: metricFlowletPath) + node.name + '&aggregate=true',
          method: 'POST'
        }, function (data) {
            // $scope.data.metrics[node.name] = data.series[0] ? data.series[0].data[0].value : 0;
            $scope.data.metrics[node.name] = myHelpers.objectQuery(data, 'series' , 0, 'data', 0, 'value') || 0;
          });
      });
    }

    $scope.stopFlow = function() {
      dataSrc.request({
        _cdapNsPath: basePath + '/stop',
        method: 'POST'
      });
    };
  });
