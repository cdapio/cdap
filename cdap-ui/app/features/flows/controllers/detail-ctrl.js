angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetail', function($scope, MyDataSource, $state, FlowDiagramData) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    FlowDiagramData.fetchData()
      .then(function(data) {
        $scope.data = data;
      });

    console.info("Polling on active Runs");
    dataSrc.poll({
      _cdapNsPath: basePath + '/runs?status=running'
    }, function(res) {
        $scope.activeRuns = res.length;
      });

    $scope.do = function(action) {
      dataSrc.request({
        _cdapNsPath: basePath + '/' + action,
        method: 'POST'
      })
    }
  });
