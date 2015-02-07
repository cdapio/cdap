angular.module(PKG.name + '.feature.services')
  .controller('ServicesLogsController', function($scope, $state, $timeout, MyDataSource) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/services/' + $state.params.programId;

    $scope.logTabs = ['all', 'info', 'warn', 'error', 'debug', 'other'];
    $scope.logs = null;

    dataSrc.poll({
      _cdapNsPath: basePath + '/logs/next?fromOffset=-1&maxSize=50'
    }, function(res) {
      console.log("Logs: ", res);
    });

    $scope.$watch('logTabs.activeTab', function(newVal, oldVal) {
      var toState;

      if (newVal) {
        // Use the new active tab clicked on the UI.
        toState = newVal;
      } else if (currentlogTab() > 0) {
        // Navigating through url. So use the log tab in the URL and go to that State.
        toState = currentlogTab();
      } else {
        // Navigating from parent. So default to 0(status).
        toState = 0;
      }
      $timeout(function() {
        $state.go('services.detail.logs.detail.' + $scope.logTabs[toState]);
      });
    });

    $scope.$on('$stateChangeSuccess', function(event, toState) {
      var tab;
      if ($state.includes('services.detail.logs.detail.*')) {
        tab = $scope.logTabs.indexOf(toState.name.split('.').slice(-1).pop())
        $scope.logTabs.activeTab = (tab > 0? tab: 0);
      }
    });

    function currentlogTab() {
      return $scope.logTabs.indexOf( $state.current.name.split('.').slice(-1).pop() );
    }

  });
