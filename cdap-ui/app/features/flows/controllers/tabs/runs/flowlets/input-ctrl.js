angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailInputController', function($state, $scope, MyDataSource, MyMetricsQueryHelper, MyChartHelpers, myFlowsApi) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.FlowletsController.activeFlowlet.name;

    var metric = {
      startTime: 'now-60s',
      endTime: 'now',
      resolution: '1s',
      names: ['system.queue.pending']
    };

    this.inputs = [];

    this.chartSettings = {
      chartMetadata: {
        showx: true,
        showy: true,
        legend: {
          show: false,
          position: 'inset'
        }
      },
      color: {
        pattern: ['red']
      },
      isLive: true,
      interval: 1000,
      aggregate: 5
    };

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

    function formatTimeseries(aggregate, series, input) {
      var processedData = MyChartHelpers.processData(
        series,
        'qid',
        metric.names,
        metric.resolution
      );
      processedData = MyChartHelpers.c3ifyData(processedData, metric, metric.names);

      var data = processedData.columns[0].slice(1);
      var format = [];

      format.unshift(aggregate - data[data.length-1]);
      for (var i = data.length - 2; i >= 0; i--) {
        format.unshift(format[0] - data[i]);
      }

      format.unshift(processedData.columns[0][0]);
      processedData.columns[0] = format;

      input.chartData = {
        x: 'x',
        columns: processedData.columns,
        keys: {
          x: 'x'
        }
      };

      input.max = Math.max.apply(Math, format.slice(1));

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

        var path = '/metrics/query?' + MyMetricsQueryHelper.tagsToParams(flowletTags) + '&metric=system.queue.pending';

        var aggregate = 0;

        dataSrc.poll({
          _cdapPath: path + '&start=now-60s&end=now&aggregate=true',
          method: 'POST',
          interval: 2000
        }, function (res) {
          // Get initial aggregate
          aggregate = res.series[0] ? res.series[0].data[0].value : 0;

          dataSrc.request({
            _cdapPath: '/metrics/query',
            method: 'POST',
            body: MyMetricsQueryHelper.constructQuery('qid', flowletTags, metric)
          })
          .then(function (seriesResult) {
            formatTimeseries(aggregate, seriesResult, input);

          });
        });

        pollArrivalRate(input);

      });
    }

  });
