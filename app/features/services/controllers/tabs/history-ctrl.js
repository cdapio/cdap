angular.module(PKG.name + '.feature.services')
  .controller('ServicesDetailHistoryController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);
    $scope.runs = null;
    dataSrc.request({
      _cdapNsPath: '/apps/' + $state.params.appId + '/services/' + $state.params.programId + '/runs'
    })
      .then(function(res) {
        $scope.runs = res;
      });

  });
