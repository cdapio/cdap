angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailOutputController', function($state, $scope, MyDataSource, myHelpers) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $state.params.flowletid;

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
              _cdapPath: '/metrics/query?context=namespace.' + $state.params.namespace
                            + '.app.' + $state.params.appId
                            + '.flow.' + $state.params.programId
                            + '.flowlet.' + $state.params.flowletid
                            + '&metric=system.process.events.out&start=now-60s&count=60',
              method: 'POST'
            }, function(res) {
              updateOutput(res.series[0].data);
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
          }
        }

      });

  });
