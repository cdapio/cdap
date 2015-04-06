angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsController', function($scope, MyDataSource, $state, $rootScope) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' +
            $state.params.appId +
            '/mapreduce/' +
            $state.params.programId;

    dataSrc.request({
      _cdapNsPath: basePath + '/runs'
    })
      .then(function(res) {
        $scope.runs = res;
        angular.forEach($scope.runs, function(value) {
          value.isOpen = $state.params.runid === value.runid;
        });
      });


    // This is for toggling (opening/closing) accordions if state changes.
    $rootScope.$on('$stateChangeSuccess', function(event, toState, toParams, fromState, fromParams) {
      if (fromState.name !== 'mapreduce.detail.runs.run' && toState.name !== 'mapreduce.detail.runs.run') {
        return;
      }
      angular.forEach($scope.runs, function(value) {
        if (value.runid === toParams.runid) {
          value.isOpen = true;
        }
        if (value.runid === fromParams.runid) {
          value.isOpen = false;
        }
      });
    });

  });
