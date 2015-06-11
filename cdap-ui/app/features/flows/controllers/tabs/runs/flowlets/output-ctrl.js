angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailOutputController', function($state, $scope, MyDataSource, MyMetricsQueryHelper, myFlowsApi) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.RunsController.activeFlowlet.name;
    var runid = $scope.runs.selected.runid;
    $scope.outputs = [];

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
            $scope.outputs.push(v.targetName);
          }
        });

        if ($scope.outputs.length > 0) {
          // OUTPUT METRICS
          dataSrc
            .poll({
              _cdapPath: '/metrics/query?' + MyMetricsQueryHelper.tagsToParams(flowletTags)
                            + '&metric=system.process.events.out&start=now-60s&count=60',
              method: 'POST'
            }, function(res) {
              updateOutput(res);
            });

          // Total
          dataSrc
            .poll({
              _cdapPath: '/metrics/query?' + MyMetricsQueryHelper.tagsToParams(flowletTags)
                            + '&metric=system.process.events.out',
              method: 'POST'
            }, function(res) {
              if (res.series[0]) {
                $scope.total = res.series[0].data[0].value;
              }
            });

        }

      });

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

      if ($scope.outputHistory) {
        $scope.outputStream = v.slice(-1);
      }

      $scope.outputHistory = [{
        label: 'output',
        values: v
      }];

    }

  });
