angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsRunsDetailController', function($scope, MyDataSource, $state, $filter) {
    var dataSrc = new MyDataSource($scope),
        filterFilter = $filter('filter'),
        basePath = '/apps/' + $state.params.appId + '/workflows/' + $state.params.programId;;
    $scope.status = null;
    $scope.duration = null;
    $scope.startTime = null;

    if ($state.params.runid) {
      var match = filterFilter($scope.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    }
    if ($scope.runs.length) {
      dataSrc.poll({
        _cdapNsPath: basePath + '/runs/' + $scope.runs.selected.runid
      }, function(res) {
          startMs = res.start * 1000;
          $scope.startTime = new Date(startMs);
          $scope.status = res.status;
          $scope.duration = (res.end ? (res.end * 1000) - startMs : 0);
      });
    }

    $scope.tabs = [{
      title: 'Status',
      template: '/assets/features/workflows/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/workflows/templates/tabs/runs/tabs/log.html'
    }];

    $scope.activeTab = $scope.tabs[0];

    $scope.$on('$destroy', function(event) {
      event.currentScope.runs.selected = null;
    });

    $scope.selectTab = function(tab) {
      $scope.activeTab = tab;
    };

  });
