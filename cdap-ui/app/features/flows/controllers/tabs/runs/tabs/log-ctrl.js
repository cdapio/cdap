angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailLogController', function($scope, $state, myFlowsApi, $timeout) {

    this.logs = [];
    if (!$scope.RunsController.runs.length) {
      return;
    }

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      runId: $scope.RunsController.runs.selected.runid,
      max: 50,
      scope: $scope
    };

    this.loadingNext = true;
    myFlowsApi.prevLogs(params)
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

      myFlowsApi.nextLogs(params)
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

      myFlowsApi.prevLogs(params)
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
