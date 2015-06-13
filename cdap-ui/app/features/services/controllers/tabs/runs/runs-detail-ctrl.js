angular.module(PKG.name + '.feature.services')
  .controller('ServicesRunDetailController', function($scope, $filter, $state) {
    var filterFilter = $filter('filter');

    var match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    $scope.RunsController.runs.selected = match[0];

  });
