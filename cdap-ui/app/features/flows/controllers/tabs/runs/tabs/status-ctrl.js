angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunsDetailStatusControler', function($state, $scope, MyDataSource, myHelpers, FlowDiagramData, $timeout, $filter) {
    var filterFilter = $filter('filter');
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    if ($state.params.runid) {
      var match = filterFilter($scope.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    }
    var metricFlowletPath = '/metrics/query?metric=system.process.events.processed' +
                          '&context=ns.' +
                          $state.params.namespace +
                          '.app.' + $state.params.appId +
                          '.flow.' + $state.params.programId +
                          '.run.' + $scope.runs.selected.runid +
                          '.flowlet.',
        metricStreamPath = '/metrics/query?metric=system.collect.events' +
                           '&context=namespace.' +
                           $state.params.namespace +
                           '.stream.';
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
    dataSrc.poll({
      _cdapNsPath: basePath + '/runs/' + $scope.runs.selected.runid
    }, function(res) {
      var startMs = res.start * 1000;
        $scope.startTime = new Date(startMs);
        $scope.status = res.status;
        $scope.duration = (res.end ? (res.end * 1000) - startMs : 0);
      });

    function pollMetrics() {
      var nodes = $scope.data.nodes;
      // Requesting Metrics data
      angular.forEach(nodes, function (node) {
        dataSrc.poll({
          _cdapPath: (node.type === 'STREAM' ? metricStreamPath: metricFlowletPath) + node.name + '&aggregate=true',
          method: 'POST'
        }, function (data) {
            $scope.data.metrics[node.name] = myHelpers.objectQuery(data, 'series' , 0, 'data', 0, 'value') || 0;
          });
      });
    }

    $scope.stopFlow = function() {
      dataSrc.request({
        _cdapNsPath: basePath + '/stop',
        method: 'POST'
      })
      .then(function() {
        $timeout(function() {
          $state.go($state.current, {}, { reload: true });
        });
      });
    };

    $scope.flowletClick = function(node) {
      $scope.selectTab($scope.tabs[1], node);
    };
  });
