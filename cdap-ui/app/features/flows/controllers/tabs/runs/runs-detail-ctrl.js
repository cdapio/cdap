angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailController', function($scope, $state, $filter) {
    var filterFilter = $filter('filter'),
        match;
    match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    // If there is no match then there is something wrong with the runid in the URL.
    $scope.RunsController.runs.selected.runid = match[0].runid;

    $scope.$on('$destroy', function() {
      $scope.RunsController.runs.selected.runid = null;
    });


  });
