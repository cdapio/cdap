angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunDetailLogsController', function ($scope, $state, myMapreduceApi, EventPipe, $timeout) {

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

    this.loadingNext = true;
    myMapreduceApi.prevLogs(params)
      .$promise
      .then(function (res) {
        this.logs = res;
        this.loadingNext = false;
      }.bind(this));

    this.loadNextLogs = function () {
      if (this.loadingNext) {
        return;
      }

      this.loadingNext = true;
      params.fromOffset = this.logs[this.logs.length-1].offset;

      myMapreduceApi.nextLogs(params)
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

      myMapreduceApi.prevLogs(params)
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
