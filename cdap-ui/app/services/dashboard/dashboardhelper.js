angular.module(PKG.name + '.services')
  .factory('DashboardHelper', function (MyDataSource, MyChartHelpers, MyMetricsQueryHelper) {
    var dataSrc = new MyDataSource();

    function startPolling (widget) {
      widget.pollId = dataSrc.poll({
        _cdapPath: '/metrics/query',
        method: 'POST',
        interval: widget.settings.interval,
        body: MyMetricsQueryHelper.constructQuery(
          'qid',
          MyMetricsQueryHelper.contextToTags(widget.metric.context),
          widget.metric
        )
      }, function (res) {

        widget.formattedData = formatData(res, widget);
      }).__pollId__;
    }


    function stopPolling (widget) {
      dataSrc.stopPoll(widget.pollId);
    }


    function startPollDashboard (dashboard) {
      angular.forEach(dashboard.columns, function (widget) {
        startPolling(widget);
      });
    }

    function stopPollDashboard (dashboard) {
      angular.forEach(dashboard.columns, function (widget) {
        stopPolling(widget);
      });
    }

    function fetchData (widget) {
      dataSrc.request({
        _cdapPath: '/metrics/query',
        method: 'POST',
        body: MyMetricsQueryHelper.constructQuery(
          'qid',
          MyMetricsQueryHelper.contextToTags(widget.metric.context),
          widget.metric
        )
      })
      .then(function (res) {
        widget.formattedData = formatData(res, widget);
      });
    }

    function fetchDataDashboard (dashboard) {
      angular.forEach(dashboard.columns, function (widget) {
        fetchData(widget);
      });
    }

    function formatData (res, widget) {
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

      return data;
    }

    return {
      startPolling: startPolling,
      stopPolling: stopPolling,
      startPollDashboard: startPollDashboard,
      stopPollDashboard: stopPollDashboard,
      fetchData: fetchData,
      fetchDataDashboard: fetchDataDashboard
    };

  });
