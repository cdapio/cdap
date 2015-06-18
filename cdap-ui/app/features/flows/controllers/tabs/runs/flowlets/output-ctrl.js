angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailOutputController', function($state, $scope, MyDataSource, MyMetricsQueryHelper, myFlowsApi) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.FlowletsController.activeFlowlet.name;
    var runid = $scope.RunsController.runs.selected.runid;
    this.outputs = [];

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    var flowletTags = {
      namespace: $state.params.namespace,
      app: $state.params.appId,
      flow: $state.params.programId,
      run: runid,
      flowlet: flowletid
    };

    myFlowsApi.get(params)
      .$promise
      .then(function (res) {

        // OUTPUTS
        angular.forEach(res.connections, function(v) {
          if (v.sourceName === flowletid) {
            this.outputs.push(v.targetName);
          }
        }.bind(this));

        if (this.outputs.length > 0) {
          // OUTPUT METRICS
          dataSrc
            .poll({
              _cdapPath: '/metrics/query?' + MyMetricsQueryHelper.tagsToParams(flowletTags)
                            + '&metric=system.process.events.out&start=now-60s&count=60',
              method: 'POST'
            }, function(res) {
              updateOutput.bind(this)(res);
            }.bind(this));

          // Total
          dataSrc
            .poll({
              _cdapPath: '/metrics/query?' + MyMetricsQueryHelper.tagsToParams(flowletTags)
                            + '&metric=system.process.events.out',
              method: 'POST'
            }, function(res) {
              if (res.series[0]) {
                this.total = res.series[0].data[0].value;
              }
            }.bind(this));

        }

      }.bind(this));

    function updateOutput(res) {
      var v = [];

      if (res.series[0]) {
        angular.forEach(res.series[0].data, function(val) {
          v.push({
            time: val.time,
            y: val.value
          });
        });
      } else {
        for (var i = 60; i > 0; i--) {
          v.push({
            time: Math.floor((new Date()).getTime()/1000 - (i)),
            y: 0
          });
        }
      }

      if (this.outputHistory) {
        this.outputStream = v.slice(-1);
      }

      this.outputHistory = [{
        label: 'output',
        values: v
      }];

    }

  });
