angular.module(PKG.name + '.feature.services')
  .controller('ServicesRunsDetailController', function($scope, $filter, $state) {
    var filterFilter = $filter('filter');
    var match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    $scope.RunsController.runs.selected.runid = match[0].runid;
    $scope.RunsController.runs.selected.status = match[0].status;
    $scope.RunsController.runs.selected.start = match[0].start;
    $scope.RunsController.runs.selected.end = match[0].end;

    $scope.$on('$destroy', function() {
      $scope.RunsController.runs.selected.runid = null;
    });

  });
