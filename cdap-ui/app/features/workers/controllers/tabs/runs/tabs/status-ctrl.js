angular.module(PKG.name + '.feature.worker')
  .controller('WorkersRunsDetailStatusController', function($state, $scope, MyDataSource, $filter) {
    var filterFilter = $filter('filter');
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/workers/' + $state.params.programId;

    if ($state.params.runid) {
      var match = filterFilter($scope.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    }

    $scope.status = null;
    $scope.duration = null;
    $scope.startTime = null;

    dataSrc.poll({
      _cdapNsPath: basePath + '/runs/' + $scope.runs.selected.runid
    }, function(res) {
      var startMs = res.start * 1000;
      $scope.startTime = new Date(startMs);
      $scope.status = res.status;
      $scope.duration = (res.end ? (res.end * 1000) - startMs : 0);
    });
  });
