angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsRunsDetailController', function($scope, $state, $filter) {
    var filterFilter = $filter('filter');
    var match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    $scope.RunsController.runs.selected = match[0];
  });
