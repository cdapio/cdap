angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceLogsController', function($scope, MyDataSource, $timeout, $state) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/mapreduce/' + $state.params.programId;

    $scope.logTabs = ['all', 'info', 'warn', 'error', 'debug', 'other'];
    $scope.logs = null;
    $scope.infoLogs = [];
    $scope.warnLogs = [];
    $scope.debugLogs = [];
    $scope.errorLogs = [];
    $scope.otherLogs = [];

    dataSrc.poll({
      _cdapNsPath: basePath + '/logs/next?fromOffset=-1&maxSize=50'
    }, function(res) {

      /*
        This should be temporary. Backend should make this filter and
        make it more 'REST' fied.
      */
      $scope.infoLogs = res.filter(function(res) {
        return res.log.indexOf('- INFO') > 0;
      })
        .map(function(logs) {
          return logs.log.trim();
        });

      $scope.debugLogs = res.filter(function(res) {
        return res.log.indexOf('- DEBUG') > 0;
      })
        .map(function(logs) {
          return logs.log.trim();
        });

      $scope.warnLogs = res.filter(function(res) {
        return res.log.indexOf('- WARN') > 0;
      })
        .map(function(logs) {
          return logs.log.trim();
        });

      $scope.errorLogs = res.filter(function(res) {
        return res.log.indexOf('- ERROR') > 0;
      })
        .map(function(logs) {
          return logs.log.trim();
        });

      $scope.otherLogs = res.filter(function(res) {
        return res.log.indexOf('- OTHER') > 0;
      })
        .map(function(logs) {
          return logs.log.trim();
        });

      $scope.allLogs = res.map(function(res) {
        res.log = res.log.replace(/[\r?\n]/g, " ");
        return res.log.trim();
      });
    });

    $scope.$watch('logTabs.activeTab', function(newVal, oldVal) {
      var toState;

      if (angular.isDefined(newVal) ) {
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
        $state.go('mapreduce.detail.logs.' + $scope.logTabs[toState]);
      });
    });

    $scope.$on('$stateChangeSuccess', function(event, toState) {
      var tab;
      if ($state.includes('mapreduce.detail.logs.*')) {
        tab = $scope.logTabs.indexOf(toState.name.split('.').slice(-1).pop())
        $scope.logTabs.activeTab = (tab > 0? tab: 0);
      }
    });

    function currentlogTab() {
      return $scope.logTabs.indexOf( $state.current.name.split('.').slice(-1).pop() );
    }

  });
