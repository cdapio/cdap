angular.module(PKG.name + '.feature.services')
  .controller('ServicesRunDetailLogController', function($scope, $state, myServiceApi) {

    this.logs = [];

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      serviceId: $state.params.programId,
      runId: $scope.RunsController.runs.selected.runid,
      max: 50,
      scope: $scope
    };

    myServiceApi.logs(params)
      .$promise
      .then(function (res) {
        this.logs = res;
      }.bind(this));

    this.loadMoreLogs = function () {
      if (this.logs.length < params.max) {
        return;
      }

      params.max += 50;

      myServiceApi.logs(params)
        .$promise
        .then(function (res) {
          this.logs = res;
        }.bind(this));
    };
  });
