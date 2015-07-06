angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunDetailLogsController', function ($scope, $state, myMapreduceApi) {

    this.logs = [];
    if (!$scope.RunsController.runs.length) {
      return;
    }

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      mapreduceId: $state.params.programId,
      runId: $scope.RunsController.runs.selected.runid,
      max: 50,
      scope: $scope
    };

    this.loading = true;
    myMapreduceApi.logs(params)
      .$promise
      .then(function (res) {
        this.logs = res;
        this.loading = false;
      }.bind(this));

    this.loadMoreLogs = function () {
      if (this.logs.length < params.max) {
        return;
      }
      this.loading = true;
      params.max += 50;

      myMapreduceApi.logs(params)
        .$promise
        .then(function (res) {
          this.logs = res;
          this.loading = false;
        }.bind(this));
    };
  });
