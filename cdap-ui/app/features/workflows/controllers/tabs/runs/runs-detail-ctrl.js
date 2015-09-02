class WorkflowsRunsDetailController {
  constructor($scope, $state, $filter) {
    let filterFilter = $filter('filter');
    let match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    $scope.RunsController.runs.selected.runid = match[0].runid;
    $scope.RunsController.runs.selected.status = match[0].status;
    $scope.RunsController.runs.selected.start = match[0].start;
    $scope.RunsController.runs.selected.end = match[0].end;
    $scope.$on('$destroy', () => { $scope.RunsController.runs.selected.runid = null;} );
  }
}
WorkflowsRunsDetailController.$inject = ['$scope', '$state', '$filter'];
angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkflowsRunsDetailController', WorkflowsRunsDetailController);
