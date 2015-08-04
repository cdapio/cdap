angular.module(PKG.name + '.feature.spark')
  .controller('SparkRunDetailLogController', function($scope, $state, mySparkApi, $timeout) {

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

    this.loadingNext = true;
    mySparkApi.prevLogs(params)
      .$promise
      .then(function (res) {
        this.logs = res;
        this.loadingNext = false;
      }.bind(this));

    this.loadNextLogs = function () {
      if (this.loadingNext) {
        return;
      }

      this.loading = true;
      params.fromOffset = this.logs[this.logs.length-1].offset;

      mySparkApi.nextLogs(params)
        .$promise
        .then(function (res) {
          this.logs = _.uniq(this.logs.concat(res));
          this.loadingNext = false;
        }.bind(this));
    };

    this.loadPrevLogs = function () {
      if (this.loadingPrev) {
        return;
      }

      this.loadingPrev = true;
      params.fromOffset = this.logs[0].offset;

      mySparkApi.prevLogs(params)
        .$promise
        .then(function (res) {
          this.logs = _.uniq(res.concat(this.logs));
          this.loadingPrev = false;

          $timeout(function() {
            document.getElementById(params.fromOffset).scrollIntoView();
          });
        }.bind(this));
    };
});
