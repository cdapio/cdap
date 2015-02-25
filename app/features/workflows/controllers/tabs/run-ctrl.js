angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsDetailRunController', function($scope, $state, MyDataSource, $timeout) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/workflows/' + $state.params.programId;

    // $scope.runTabs = ['flow', 'data', 'configuration', 'log'];
    $scope.runTabs = ['flow', 'log'];

    $scope.runs = null;
    $scope.currentRun = null;

    dataSrc.request({
      _cdapNsPath: basePath + '/runs'
    })
      .then(function(res) {
        $scope.runs = res;
        if (res.length === 0) {
          $scope.currentRun = 'dormant-run';
        } else {
          $scope.currentRun = $state.params.runId || res[0].runid;
        }
      });

    $scope.$watch('runTabs.activeTab', function(newVal, oldVal) {
      var toState;

      if (angular.isDefined(newVal)) {
        // Use the new active tab clicked on the UI.
        toState = newVal;
      } else if (currentRunTab() > 0) {
        // Navigating through url. So use the run tab in the URL and go to that State.
        toState = currentRunTab();
      } else {
        // Navigating from parent. So default to 0(status).
        toState = 0;
      }
      $timeout(function() {
        $state.go('workflows.detail.runs.detail.' + $scope.runTabs[toState]);
      });
    });

    $scope.$watch('currentRun', function(newVal, oldVal) {
      var toState, currentTab;
      if(newVal) {
        // If already in a runTab and you switched the runId then go to the corresponding state.
        // Instance: If already in flows.detail.runs.detail.data and you chose a different runId then go to that state with new runId
        if ($state.includes('**.runs.detail.**')) {
          toState = $state.current;
        } else {
          // Else default to status state if navigating from parent.
          toState = 'workflows.detail.runs.detail.flow';
        }
        $timeout(function() {
          $state.go(toState, {
            runId: newVal
          });
        });
      } else {
        // Navigating using URL. Use the already existing runId and set the tab if present in the URL.
        $scope.currentRun = $state.params.runId;
        currentTab = currentRunTab();
        $scope.runTabs.activeTab =  (currentTab > 0 ? currentTab : 0);
      }
    });

    $scope.$on('$stateChangeSuccess', function(event, toState) {
      var tab;
      if ($state.includes('workflows.detail.runs.detail.*')) {
        tab = $scope.runTabs.indexOf(toState.name.split('.').slice(-1).pop());
        $scope.runTabs.activeTab = (tab > 0? tab: 0);
      }
    });

    function currentRunTab() {
      return $scope.runTabs.indexOf( $state.current.name.split('.').slice(-1).pop() );
    }
  });
