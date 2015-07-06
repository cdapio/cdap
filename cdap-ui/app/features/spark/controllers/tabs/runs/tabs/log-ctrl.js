angular.module(PKG.name + '.feature.spark')
  .controller('SparkRunDetailLogController', function($scope, $state, mySparkApi) {

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      sparkId: $state.params.programId,
      runId: $scope.RunsController.runs.selected.runid,
      max: 50,
      scope: $scope
    };

    if (!$scope.RunsController.runs.length) {
      return;
    }

    mySparkApi.logs(params)
      .$promise
      .then(function (res) {
        this.logs = res;
      }.bind(this));

    this.loadMoreLogs = function () {
      if (this.logs.length < params.max) {
        return;
      }

      params.max += 50;

      mySparkApi.logs(params)
        .$promise
        .then(function (res) {
          this.logs = res;
        }.bind(this));
    };
});
