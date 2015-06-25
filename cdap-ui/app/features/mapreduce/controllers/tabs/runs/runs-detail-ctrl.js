angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsDetailController', function($scope, $state, $filter) {

    var filterFilter = $filter('filter');
    var match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    $scope.RunsController.runs.selected.runid = match[0].runid;

    $scope.$on('$destroy', function() {
      $scope.RunsController.runs.selected.runid = null;
    });

  });
