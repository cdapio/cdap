angular.module(PKG.name + '.feature.spark')
  .controller('SparkRunsDetailStatusControler', function($state, $scope, MyDataSource, myHelpers, $timeout, $filter) {
    var filterFilter = $filter('filter');
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/spark/' + $state.params.programId;

    if ($state.params.runid) {
      var match = filterFilter($scope.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    }
    $scope.data = {
      running: 33.3,
      failed: 33.3,
      waiting: 33.3
    };

    $scope.runningTooltip = {
      "title": 'Running'
    };

    $scope.waitingTooltip = {
      "title": 'Waiting'
    };

    $scope.failedTooltip = {
      "title": 'Failed'
    };

    $scope.status = null;
    $scope.duration = null;
    $scope.startTime = null;

    // This controller is NOT shared between the accordions.
    dataSrc.poll({
      _cdapNsPath: basePath + '/runs/' + $scope.runs.selected.runid
    }, function(res) {
      var startMs = res.start * 1000;
        $scope.startTime = new Date(startMs);
        $scope.status = res.status;
        $scope.duration = (res.end ? (res.end * 1000) - startMs : 0);
      });
  });
