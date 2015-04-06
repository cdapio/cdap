angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsRunsController', function($scope, $state, MyDataSource, $timeout, $rootScope) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/workflows/' + $state.params.programId;;

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
      if (fromState.name !== 'workflows.detail.runs.run' && toState.name !== 'workflows.detail.runs.run') {
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
