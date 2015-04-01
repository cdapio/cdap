angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunsController', function($scope, MyDataSource, $state, $rootScope, FlowDiagramData) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;
    $scope.runs = [];

    dataSrc.request({
      _cdapNsPath: basePath + '/runs'
    }, function(res) {
        $scope.runs = res;
        angular.forEach($scope.runs, function(value) {
          value.isOpen = $state.params.runid === value.runid;
        });
      });


    // This is for toggling (opening/closing) accordions if state changes.
    $rootScope.$on('$stateChangeSuccess', function(event, toState, toParams, fromState, fromParams) {
      if (fromState.name !== 'flows.detail.runs.run' && toState.name !== 'flows.detail.runs.run') {
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



  })
