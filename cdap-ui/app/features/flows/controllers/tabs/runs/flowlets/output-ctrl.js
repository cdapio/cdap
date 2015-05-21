angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailOutputController', function($state, $scope, MyDataSource, myHelpers) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.$parent.activeFlowlet.name;
    var runid = $scope.runs.selected.runid;
    $scope.outputs = [];

    var flowletTags = {
      namespace: $state.params.namespace,
      app: $state.params.appId,
      flow: $state.params.programId,
      run: runid,
      flowlet: flowletid
    };

    // Initialize
    dataSrc
      .request({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId
      })
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
              _cdapPath: '/metrics/query?' + myHelpers.tagsToParams(flowletTags)
                            + '&metric=system.process.events.out&start=now-60s&count=60',
              method: 'POST'
            }, function(res) {
              if (res.series[0]) {
                updateOutput(res.series[0].data);
              } else {
                  var val = [];

                  for (var i = 60; i > 0; i--) {
                    val.push({
                      time: Math.floor((new Date()).getTime()/1000 - (i)),
                      y: 0
                    });
                  }

                  if ($scope.outputHistory) {
                    $scope.outputStream = val.slice(-1);
                  }

                  $scope.outputHistory = [{
                    label: 'output',
                    values: val
                  }];

                }
            });

          function updateOutput(newVal) {
            if(angular.isObject(newVal)) {
              var v = [];

              angular.forEach(newVal, function(val) {
                v.push({
                  time: val.time,
                  y: val.value
                });
              });

              if ($scope.outputHistory) {
                $scope.outputStream = v.slice(-1);
              }

              $scope.outputHistory = [
                {
                  label: 'output',
                  values: v
                }
              ];

            }

            // Total
            dataSrc
              .poll({
                _cdapPath: '/metrics/query?' + myHelpers.tagsToParams(flowletTags)
                              + '&metric=system.process.events.out',
                method: 'POST'
              }, function(res) {
                if (res.series[0]) {
                  $scope.total = res.series[0].data[0].value;
                }
              });

          }

        }

      });

  });
