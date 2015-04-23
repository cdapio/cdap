angular.module(PKG.name + '.feature.spark')
  .controller('SparkRunDetailLogController', function($scope, MyDataSource, $state) {

    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId +
                   '/spark/' + $state.params.programId +
                   '/runs/' + $scope.runs.selected.runid;

    $scope.logs = [];

    dataSrc.poll({
      _cdapNsPath: basePath + '/logs/next?max=50'
    }, function(res) {
      $scope.logs = res;
    });

});
