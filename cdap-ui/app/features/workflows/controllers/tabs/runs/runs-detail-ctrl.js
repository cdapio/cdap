class WorkflowsRunsDetailController {
  constructor($scope, $state, $filter) {
    let filterFilter = $filter('filter');
    let match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    $scope.RunsController.runs.selected.runid = match[0].runid;

    $scope.$on('$destroy', () => { $scope.RunsController.runs.selected.runid = null;} );
  }
}
WorkflowsRunsDetailController.$inject = ['$scope', '$state', '$filter'];
angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkflowsRunsDetailController', WorkflowsRunsDetailController);
