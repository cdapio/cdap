angular.module(PKG.name + '.feature.services')
  .controller('ServicesRunDetailController', function($scope, $filter, $state) {
    var filterFilter = $filter('filter');

    var match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    $scope.RunsController.runs.selected.runid = match[0].runid;

    $scope.$on('$destroy', function() {
      $scope.RunsController.runs.selected.runid = null;
    });

  });
