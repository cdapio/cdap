angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsController', function($scope, MyDataSource, $state, $rootScope) {

    var dataSrc = new MyDataSource($scope);

    dataSrc.request({
      _cdapNsPath: '/apps/' + $state.params.appId + '/mapreduce/' + $state.params.programId + '/runs'
    })
    .then(function (res) {
      $scope.runs = res;

      if (res.length > 0) {
        if ($state.params.runid) {
          $scope.current = $state.params.runid;
        } else {
          $scope.current = res[0].runid;
        }
      }
    });

    $rootScope.$on('$stateChangeSuccess', function() {
      if ($state.params.runid) {
        $scope.current = $state.params.runid;
      }
    });

  });
