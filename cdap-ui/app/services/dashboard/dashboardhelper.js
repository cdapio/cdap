angular.module(PKG.name + '.services')
  .factory('DashboardHelper', function (MyDataSource, MyChartHelpers, MyMetricsQueryHelper) {
    var dataSrc = new MyDataSource();

    function startPolling (widget) {
      dataSrc.poll({
        _cdapPath: '/metrics/query',
        method: 'POST',
        body: MyMetricsQueryHelper.constructQuery(
          'qid',
          MyMetricsQueryHelper.contextToTags(widget.metric.context),
          widget.metric
        )
      }, function (res) {
        var processedData = MyChartHelpers.processData(
          res,
          'qid',
          widget.metric.names,
          widget.metric.resolution,
          widget.settings.aggregate
        );

        processedData = MyChartHelpers.c3ifyData(processedData, widget.metric, widget.metricAlias);
        var data = {
          x: 'x',
          columns: processedData.columns,
          keys: {
            x: 'x'
          }
        };
        widget.formattedData = data;

        widget.pollId = res.__pollId__;
      });
    }


    function stopPolling (widget) {
      dataSrc.stopPoll(widget.pollId);
    }


    function startPollDashboard (dashboard) {
      angular.forEach(dashboard.columns, function (widget) {
        startPolling(widget);
      });
    }

    function stopPollingDashboard (dashboard) {
      angular.forEach(dashboard.columns, function (widget) {
        stopPolling(widget);
      });
    }

    return {
      startPolling: startPolling,
      stopPolling: stopPolling,
      startPollDashboard: startPollDashboard,
      stopPollingDashboard: stopPollingDashboard
    };

  });
