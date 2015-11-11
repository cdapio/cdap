/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.services')
  .factory('DashboardHelper', function (MyCDAPDataSource, MyChartHelpers, MyMetricsQueryHelper) {
    var dataSrc = new MyCDAPDataSource();

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
      return dataSrc.request({
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

    function pollData (widget) {
      return dataSrc.poll({
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
      pollData: pollData,
      fetchDataDashboard: fetchDataDashboard
    };

  });
