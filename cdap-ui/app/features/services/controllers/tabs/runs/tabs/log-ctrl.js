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

    this.loading = true;
    myServiceApi.logs(params)
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

      myServiceApi.logs(params)
        .$promise
        .then(function (res) {
          this.logs = res;
          this.loading = false;
        }.bind(this));
    };
  });
