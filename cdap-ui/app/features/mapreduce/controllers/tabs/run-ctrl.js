angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsController', function($scope, MyDataSource, $state, $rootScope, rRuns) {

    var dataSrc = new MyDataSource($scope);

    $scope.runs = rRuns;

    if (!$state.params.runid) {
      if ($scope.runs.length === 0) {
        $scope.current = 'No Run';
      } else {
        $scope.current = rRuns[0].runid;
      }
    } else {
      $scope.current = $state.params.runid;
    }

    $rootScope.$on('$stateChangeSuccess', function() {
      if ($state.params.runid) {
        $scope.current = $state.params.runid;
      } else if ($scope.runs.length === 0) {
        $scope.current = 'No Run';
      } else {
        $scope.current = $scope.runs[0].runid;
      }
    });

  });
