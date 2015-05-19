angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailStatusController', function($state, $scope, MyDataSource, myHelpers, FlowDiagramData, $timeout) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    var metricStreamPath = '/metrics/query?metric=system.collect.events' +
                           '&context=namespace.' +
                           $state.params.namespace +
                           '.stream.';
    $scope.data = {};

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then(function(data) {
        $scope.data = data;
        pollMetrics();
      });

    if ($scope.runs.length) {
      var metricFlowletPath = '/metrics/query?metric=system.process.events.processed' +
                            '&context=ns.' +
                            $state.params.namespace +
                            '.app.' + $state.params.appId +
                            '.flow.' + $state.params.programId +
                            '.run.' + $scope.runs.selected.runid +
                            '.flowlet.';
    }

    function pollMetrics() {
      var nodes = $scope.data.nodes;
      $scope.data.instances = {};
      // Requesting Metrics data
      angular.forEach(nodes, function (node) {
        if (node.type !== 'STREAM' && !$scope.runs.length) {
          return;
        }
        dataSrc.poll({
          _cdapPath: (node.type === 'STREAM' ? metricStreamPath: metricFlowletPath) + node.name + '&aggregate=true',
          method: 'POST'
        }, function (data) {
            $scope.data.metrics[node.name] = myHelpers.objectQuery(data, 'series' , 0, 'data', 0, 'value') || 0;
          });

        if (node.type !== 'STREAM') {
          dataSrc.poll({
            _cdapNsPath: '/apps/' + $state.params.appId +  '/flows/' + $state.params.programId + '/flowlets/' + node.name + '/instances'
          }, function (res) {
            $scope.data.instances[node.name] = res.instances;
          });
        }


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
