angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailInputController', function($state, $scope, MyDataSource) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.$parent.activeFlowlet.name;

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
                              + '.consumer.' + flowletid
                              + '.producer.' + input.name
                              + '&metric=system.queue.pending',
                streamPath = '/metrics/query?context=namespace.' + $state.params.namespace
                              + '.stream.' + input.name
                              + '&metric=system.collect.events&start=now-60s';

            var path = '';
            if (input.sourceType === 'STREAM') {
              path = streamPath;
            } else {
              path = flowletPath;
            }
            var aggregate = 0;
            // Get Aggregate
            dataSrc.request({
              _cdapPath: path + '&start=now-60s&end=now&aggregate=true',
              method: 'POST'
            }).then(function(res) {
              aggregate = res.series[0] ? res.series[0].data[0].value : 0;
            }).then(function() {

              dataSrc.request({
                _cdapPath: path + '&start=now-60s&end=now',
                method: 'POST'
              }).then(function(initial) {
                if (initial.series[0]) {
                  var response = initial.series[0].data;
                  var v = [];

                  response[response.length - 1].value = aggregate - response[response.length - 1].value;
                  for (var i = response.length - 2; i >= 0; i--) {
                    response[i].value = response[i+1].value - response[i].value;
                    v.unshift({
                      time: response[i].time,
                      y: response[i].value
                    });
                  }

                  input.stream = v.slice(-1);

                  input.history = [
                    {
                      label: 'output',
                      values: v
                    }
                  ];

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
              }).then(function() {
                // start polling aggregate

                dataSrc.poll({
                  _cdapPath: path + '&start=now-60s&end=now&aggregate=true',
                  method: 'POST',
                  interval: 1000
                }, function(streamData) {
                  var stream = {};

                  if (streamData.series[0]) {
                    stream = {
                      time: Math.floor((new Date()).getTime()/1000),
                      y: streamData.series[0].data[0].value
                    };

                  } else {

                    stream = {
                      time: Math.floor((new Date()).getTime()/1000),
                      y: 0
                    };
                  }

                  var array = input.history[0].values;
                  array.shift();
                  array.push(stream);

                  input.history = [
                    {
                      label: 'output',
                      values: array
                    }
                  ];

                  input.stream = array.slice(-1);

                  input.max = Math.max.apply(Math, array.map(function(o){return o.y;}));

                });
              });

            });

            var arrivalPath = '/metrics/query?metric=system.process.events.processed'+
              '&tag=namespace:' + $state.params.namespace +
              '&tag=app:' + $state.params.appId +
              '&tag=flow' + $state.params.programId +
              '&tag=flowlet:' + flowletid +
              '&tag=run:' + $scope.runs.selected.runid +
              '&start=now-1s&end=now';
            // POLLING ARRIVAL RATE
            dataSrc
              .poll({
                _cdapPath: arrivalPath,
                method: 'POST'
              }, function (res) {
                console.log('RES', res);
                if (res.series[0]) {
                  input.total = res.series[0].data[0].value;
                }
              });

          });

        }

      });
  });
