angular.module(PKG.name + '.feature.spark')
  .controller('SparkRunDetailLogController', function($scope, $state, mySparkApi) {

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      sparkId: $state.params.programId,
      runId: $scope.runs.selected.runid,
      max: 50,
      scope: $scope
    };

    mySparkApi.logs(params)
      .$promise
      .then(function (res) {
        $scope.logs = res;
      });

});
