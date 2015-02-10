angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailController', function($scope, $state, $timeout, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' +
            $state.params.appId +
            '/flows/' +
            $state.params.programId;

    $scope.activeRuns = 0;

    dataSrc.poll({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        var count = 0;
        angular.forEach(res, function(runs) {
          if (runs.status === 'RUNNING') {
            count += 1;
          }
        });
        $scope.activeRuns = count;
      });

    dataSrc.poll({
      _cdapNsPath: basePath + '/status'
    }, function(res) {
      $scope.status = res.status;
    });

    $scope.toggleFlow = function(action) {
      $scope.status = action;
      dataSrc.request({
        method: 'POST',
        _cdapNsPath: basePath + '/' + action
      });
    };

});
