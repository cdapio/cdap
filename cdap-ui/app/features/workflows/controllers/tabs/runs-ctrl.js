angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsRunsController', function($scope, $state, $filter, rRuns) {
    var fFilter = $filter('filter');
    $scope.runs = rRuns;

    if ($state.params.runid) {
      var match = fFilter(rRuns, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    } else {
      $scope.runs.selected = rRuns[0];
    }

    $scope.tabs = [{
      title: 'Status',
      template: '/assets/features/workflows/templates/tabs/runs/tabs/status.html'
    }];
  });
