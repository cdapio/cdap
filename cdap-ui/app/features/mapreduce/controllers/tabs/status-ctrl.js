angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceStatusController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);

    dataSrc.request({
      _cdapNsPath: '/apps/' + $state.params.appId + '/mapreduce/' + $state.params.programId + '/runs'
    })
    .then(function (res) {
      $scope.runs = res;

      if (res.length > 0 && $state.params.runid) {
        $scope.current = res[0].runid;
      } else if ($state.params.runid) {
        $scope.current = $
      }
    });

    // $scope.template = '/assets/features/mapreduce/templates/tabs/runs/tabs/status.html';

  });
