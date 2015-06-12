angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailInputController', function($state, $scope, MyDataSource, MyMetricsQueryHelper, myFlowsApi, myAlert) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.FlowletsController.activeFlowlet.name;

    this.inputs = [];

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    myFlowsApi.get(params)
      .$promise
      .then(function (res) {
        // INPUTS
        angular.forEach(res.connections, function(v) {
          if (v.targetName === flowletid) {
            this.inputs.push({
              name: v.sourceName,
              max: 0,
              type: v.sourceType
            });
          }
        }.bind(this));

        if (this.inputs.length > 0) {
          formatInput.bind(this)();
        }
      }.bind(this));

    function pollArrivalRate(input) {
      var arrivalPath = '/metrics/query?metric=system.process.events.processed'+
        '&tag=namespace:' + $state.params.namespace +
        '&tag=app:' + $state.params.appId +
        '&tag=flow' + $state.params.programId +
        '&tag=flowlet:' + flowletid +
        '&tag=run:' + $scope.RunsController.runs.selected.runid +
        '&start=now-1s&end=now';
      // TODO: should this value be averaged over more than just the past 1 second?
      // POLLING ARRIVAL RATE
      dataSrc
        .poll({
          _cdapPath: arrivalPath,
          method: 'POST'
        }, function (res) {
          if (res.series[0]) {
            input.total = res.series[0].data[0].value;
          }
        });
    }

    function formatInitialTimeseries(aggregate, initial, input) {
      var v = [];

      if (initial.series[0]) {
        var response = initial.series[0].data;

        response[response.length - 1].value = aggregate - response[response.length - 1].value;
        for (var i = response.length - 2; i >= 0; i--) {
          response[i].value = response[i+1].value - response[i].value;
          v.unshift({
            time: response[i].time,
            y: response[i].value
          });
        }
      } else {
        // when there is no data
        for (var i = 60; i > 0; i--) {
          v.push({
            time: Math.floor((new Date()).getTime()/1000 - (i)),
            y: 0
          });
        }
      }

      input.stream = v.slice(-1);

      input.history = [{
        label: 'output',
        values: v
      }];
    }

    function pollAggregateQueue(path, input) {
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
    }

    function formatInput() {
      angular.forEach(this.inputs, function (input) {
        var flowletTags = {
          namespace: $state.params.namespace,
          app: $state.params.appId,
          flow: $state.params.programId,
          consumer: flowletid,
          producer: input.name
        };

        var path = '/metrics/query?' + MyMetricsQueryHelper.tagsToParams(flowletTags)
                          + '&metric=system.queue.pending';

        var aggregate = 0;
        // Get Aggregate
        dataSrc.request({
          _cdapPath: path + '&start=now-60s&end=now&aggregate=true',
          method: 'POST'
        }).then(function(res) {

          // Get initial aggregate
          aggregate = res.series[0] ? res.series[0].data[0].value : 0;

          // Get timeseries
          return dataSrc.request({
            _cdapPath: path + '&start=now-60s&end=now',
            method: 'POST'
          });

        }).then(function(initial) {
          formatInitialTimeseries(aggregate, initial, input);

          // start polling aggregate
          pollAggregateQueue(path, input);

        });

        pollArrivalRate(input);

      });
    }

  });
