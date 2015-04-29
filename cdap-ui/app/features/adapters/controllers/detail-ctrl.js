angular.module(PKG.name + '.feature.adapters')
  .controller('AdpaterDetailController', function($scope, rRuns, $state, MyDataSource) {
    var dataSrc = new MyDataSource($scope);
    var path = '/adapters/' + $state.params.adapterId;

    $scope.start = function() {
      $scope.status = 'STARTING';

      var requestObj = {
        _cdapNsPath: path + '/start',
        method: 'POST'
      };

      if ($scope.runtimeArgs && Object.keys($scope.runtimeArgs).length > 0) {
        requestObj.body = $scope.runtimeArgs;
      }

      dataSrc.request(requestObj)
        .then(function() {
          $state.go($state.current, $state.params, {reload: true});
        });
    };

    $scope.stop = function() {
      $scope.status = 'STOPPING';
      dataSrc.request({
        _cdapNsPath: path + '/stop',
        method: 'POST'
      })
        .then(function() {
          $state.go($state.current, $state.params, {reload: true});
        });
    };

    dataSrc.poll({
      _cdapNsPath: path + '/status'
    }, function(res) {
      // Comment it out https://issues.cask.co/browse/CDAP-2346 when fixed.
      // $scope.status = res.status;
      $scope.status = res;
    });

  });
