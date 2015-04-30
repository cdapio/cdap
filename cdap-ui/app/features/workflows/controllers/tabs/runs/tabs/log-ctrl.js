angular.module(PKG.name + '.feature.workflows')
  .controller('WorkFlowsRunDetailLogController', function($scope, MyDataSource, $state) {

    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId +
                   '/workflows/' + $state.params.programId +
                   '/runs/' + $scope.runs.selected.runid;

    $scope.logs = [];
    if (!$scope.runs.length) {
      return;
    }
    dataSrc.poll({
      _cdapNsPath: basePath + '/logs/next?max=50'
    }, function(res) {
      $scope.logs = res;
    });

});
