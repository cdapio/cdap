angular.module(PKG.name + '.feature.worker')
  .controller('WorkersRunsDetailStatusController', function($state, $scope, MyDataSource, $filter) {
    var filterFilter = $filter('filter');
    
    if ($state.params.runid) {
      var match = filterFilter($scope.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    }

  });
