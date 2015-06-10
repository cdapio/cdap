angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunDetailLogsController', function ($scope, $state, myMapreduceApi) {

    $scope.logs = [];
    if (!$scope.runs.length) {
      return;
    }

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      mapreduceId: $state.params.programId,
      runId: $scope.current,
      max: 50,
      scope: $scope
    };

    myMapreduceApi.logs(params)
      .$promise
      .then(function (res) {
        $scope.logs = res;
      });
  });
