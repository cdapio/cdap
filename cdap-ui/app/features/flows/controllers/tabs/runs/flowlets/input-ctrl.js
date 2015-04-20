angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailInputController', function($state, $scope, MyDataSource, myHelpers) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.$parent.activeFlowlet.name;
    var runid = $scope.runs.selected.runid;

    $scope.inputs = [];

    // Initialize
    dataSrc
      .request({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId
      })
      .then(function (res) {

        // INPUTS
        angular.forEach(res.connections, function(v) {
          if (v.targetName === flowletid) {
            $scope.inputs.push({
              name: v.sourceName,
              max: 0,
              type: v.sourceType
            });
          }
        });

        if ($scope.inputs.length > 0) {

          angular.forEach($scope.inputs, function (input) {
            var flowletPath = '/metrics/query?context=namespace.' + $state.params.namespace
                              + '.app.' + $state.params.appId
                              + '.flow.' + $state.params.programId
                              + '.run.' + runid
                              + '.flowlet.' + input.name
                              + '&metric=system.process.events.out&start=now-60s',
                streamPath = '/metrics/query?context=namespace.' + $state.params.namespace
                              + '.stream.' + input.name
                              + '&metric=system.collect.events&start=now-60s';

            var path = '';
            if (input.sourceType === 'STREAM') {
              path = streamPath;
            } else {
              path = flowletPath;
            }

            // POLLING GRAPH
            dataSrc
              .poll({
                _cdapPath: path + '&count=60',
                method: 'POST'
              }, function (res) {

                if(res.series[0]) {
                  var response = res.series[0].data;
                  var v = [];

                  angular.forEach(response, function(val) {
                    v.push({
                      time: val.time,
                      y: val.value
                    });
                  });

                  if (input.history) {
                    input.stream = v.slice(-1);
                  }

                  input.history = [
                    {
                      label: 'output',
                      values: v
                    }
                  ];

                  input.max = Math.max.apply(Math, v.map(function(o){return o.y;}));
                } else {
                  var val = [];

                  for (var i = 60; i > 0; i--) {
                    val.push({
                      time: Math.floor((new Date()).getTime()/1000 - (i)),
                      y: 0
                    });
                  }

                  if (input.history) {
                    input.stream = val.slice(-1);
                  }

                  input.history = [{
                    label: 'output',
                    values: val
                  }];

                }

              });

            // POLLING ARRIVAL RATE
            dataSrc
              .poll({
                _cdapPath: path + '&count=1',
                method: 'POST'
              }, function (res) {
                if (res.series[0]) {
                  input.total = res.series[0].data[0].value / 60;
                }
              });

          });

        }

      });
  });
