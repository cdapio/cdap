angular.module(PKG.name + '.feature.services')
  .controller('ServicesDetailController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope),
        path = '/apps/' +
          $state.params.appId + '/services/' +
          $state.params.programId;

    $scope.start = function() {
      $scope.status = 'STARTING';
      dataSrc.request({
        _cdapNsPath: path + '/start',
        method: 'POST'
      });
    };

    $scope.stop = function() {
      $scope.status = 'STOPPING';
      dataSrc.request({
        _cdapNsPath: path + '/stop',
        method: 'POST'
      });
    };

    dataSrc.request({
      _cdapNsPath: path + '/status'
    }, function(res) {
      $scope.status = res.status || 'Unknown';
    });
  });
