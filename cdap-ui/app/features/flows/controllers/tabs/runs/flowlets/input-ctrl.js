angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailInputController', function($state, $scope, MyDataSource, myHelpers) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $state.params.flowletid;
    // Initialize
    dataSrc
      .request({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId
      })
      .then(function (res) {

        // INPUTS
        angular.forEach(res.connections, function(v) {
          if (v.targetName === flowletid) {
            $scope.inputs.push(v.sourceName);
          }
        });

        if ($scope.inputs.length > 0) {
          // INPUT METRICS
          dataSrc
            .poll({
              _cdapPath: '/metrics/query?context=namespace.' + $state.params.namespace
                            + '.app.' + $state.params.appId
                            + '.flow.' + $state.params.programId
                            + '.flowlet.' + $scope.inputs[0]
                            + '&metric=system.process.events.out&start=now-60s&count=60',
              method: 'POST'
            }, function (res) {
              updateInput(res.series[0].data);
            });

          function updateInput(newVal) {
            if(angular.isObject(newVal)) {
              var v = [];

              angular.forEach(newVal, function(val) {
                v.push({
                  time: val.time,
                  y: val.value
                });
              });

              if ($scope.inputHistory) {
                $scope.inputStream = v.slice(-1);
              }

              $scope.inputHistory = [
                {
                  label: 'output',
                  values: v
                }
              ];

            }
          }
        }

      });
  });
