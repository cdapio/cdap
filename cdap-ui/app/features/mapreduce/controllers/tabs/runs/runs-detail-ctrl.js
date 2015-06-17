angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsDetailController', function($scope, $state, $filter, myMapreduceApi) {

    var filterFilter = $filter('filter');
    var match = filterFilter($scope.RunsController.runs, {runid: $state.params.runid});
    $scope.RunsController.runs.selected = match[0];

  });
