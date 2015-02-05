angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailRunController', function($scope, $state, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;
    $scope.runTabs = ['status', 'flowlets', 'data', 'configuration', 'log'];
    $scope.runs = null;
    $scope.currentRun = null;
    dataSrc.request({
      _cdapNsPath: basePath + '/runs'
    })
      .then(function(res) {
        $scope.runs = res;
        if (res.length === 0) {
          $scope.currentRun = 'dormant-flow';
        } else {
          $scope.currentRun = $state.params.runId || res[0].runid;
        }
      });

    $scope.$watch('runTabs.activeTab', function(newVal, oldVal) {
      $state.go('flows.detail.runs.detail.' + $scope.runTabs[newVal || 0]);
    });

    $scope.$watch('currentRun', function(newVal, oldVal) {
      if(newVal) {
        if ($state.includes('flows.detail.runs.detail.status')) {
          $state.go($state.current, {
            runId: newVal
          });
        } else {
          $state.go('flows.detail.runs.detail.status', {
            runId: newVal
          });
        }
      } else {
        $scope.currentRun = $state.params.runId;
        $scope.runTabs.activeTab = 0;
      }
    });
  });
