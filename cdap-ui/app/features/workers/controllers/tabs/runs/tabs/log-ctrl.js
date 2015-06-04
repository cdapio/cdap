angular.module(PKG.name + '.feature.worker')
  .controller('WorkersRunDetailLogController', function($scope, MyDataSource, $state) {

    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId +
                   '/workers/' + $state.params.programId +
                   '/runs/' + $scope.runs.selected.runid;

    $scope.logs = [];

    dataSrc.poll({
      _cdapNsPath: basePath + '/logs/next?max=50'
    }, function(res) {
      $scope.logs = res;
    });

});
