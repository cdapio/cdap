angular.module(PKG.name + '.feature.worker')
  .controller('WorkersDetailController', function($scope, MyDataSource, $state, MY_CONFIG) {
    var dataSrc = new MyDataSource($scope),
        path = '/apps/' +
          $state.params.appId + '/workers/' +
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

    dataSrc.poll({
      _cdapNsPath: path + '/status'
    }, function(res) {
      $scope.status = res.status;
    });
  });
