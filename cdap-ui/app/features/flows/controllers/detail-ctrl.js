angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailController', function($scope, MyDataSource, $state, myProgramPreferencesService, myRuntimeService ) {
    var dataSrc = new MyDataSource($scope),
        path = '/apps/' +
          $state.params.appId + '/flows/' +
          $state.params.programId;

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
      $scope.status = res.status;
    });

    $scope.openPreferences = function() {
      myProgramPreferencesService.show('flows');
    };

    $scope.openRuntime = function() {
      myRuntimeService.show().result.then(function(res) {
        $scope.runtimeArgs = res;
      });
    };
  });
