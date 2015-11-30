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
  .factory('MyChartHelpers', function(myHelpers, MyMetricsQueryHelper) {

    function processData (queryResults, queryId, metricNames, metricResolution, isAggregate) {
      var metrics, metric, data, dataPt, result;
      var i, j;
      var tempMap = {};
      var tmpData = [];
      result = queryResults[queryId];
      // metrics = this.metric.names;
      metrics = metricNames;
      for (i = 0; i < metrics.length; i++) {
        metric = metrics[i];
        tempMap[metric] = zeroFill(metricResolution, result);
      }
      for (i = 0; i < result.series.length; i++) {
        data = result.series[i].data;
        metric = result.series[i].metricName;
        for (j = 0 ; j < data.length; j++) {
          dataPt = data[j];
          tempMap[metric][dataPt.time] = dataPt.value;
        }
      }
      for (i = 0; i < metrics.length; i++) {
        var thisMetricData = tempMap[metrics[i]];
        if (isAggregate) {
          thisMetricData = MyMetricsQueryHelper.aggregate(thisMetricData, isAggregate);
        }
        tmpData.push(thisMetricData);
      }
      // this.data = tmpData;
      return tmpData;
    }

    // Compute resolution since back-end doesn't provide us the resolution when 'auto' is used
    function resolutionFromAuto(startTime, endTime) {
      var diff = endTime - startTime;
      if (diff <= 600) {
        return '1s';
      } else if (diff <= 36000) {
        return '1m';
      }
      return '1h';
    }

    function skipAmtFromResolution(resolution) {
      switch(resolution) {
        case '1h':
          return 60 * 60;
        case '1m':
            return 60;
        case '1s':
            return 1;
        default:
            // backend defaults to '1s'
            return 1;
      }
    }

    function zeroFill(resolution, result) {
      // interpolating (filling with zeros) the data since backend returns only metrics at specific time periods
      // instead of for the whole range. We have to interpolate the rest with 0s to draw the graph.
      if (resolution === 'auto') {
        resolution = resolutionFromAuto(result.startTime, result.endTime);
      }
      var skipAmt = skipAmtFromResolution(resolution);

      var startTime = MyMetricsQueryHelper.roundUpToNearest(result.startTime, skipAmt);
      var endTime = MyMetricsQueryHelper.roundDownToNearest(result.endTime, skipAmt);
      var tempMap = {};
      for (var j = startTime; j <= endTime; j += skipAmt) {
        tempMap[j] = 0;
      }
      return tempMap;
    }

    function c3ifyData (newVal, metrics, alias) {
      var columns,
          totals,
          metricNames,
          metricAlias,
          values,
          xCoords;
      if(angular.isObject(newVal) && newVal.length) {

        metricNames = metrics.names.map(function(metricName) {
          metricAlias = alias[metricName];
          if (metricAlias !== undefined) {
            metricName = metricAlias;
          }
          return metricName;
        });


        // columns will be in the format: [ [metric1Name, v1, v2, v3, v4], [metric2Name, v1, v2, v3, v4], ... xCoords ]
        columns = [];
        newVal.forEach(function (value, index) {
          values = Object.keys(value).map(function(key) {
            return value[key];
          });
          values.unshift(metricNames[index]);
          columns.push(values);
        });

        // x coordinates are expected in the format: ['x', ts1, ts2, ts3...]
        xCoords = Object.keys(newVal[0]);
        xCoords.unshift('x');
        columns.push(xCoords);

        totals = [];
        columns.forEach(function(column) {
          if (!column.length || column[0] === 'x') {
            return;
          }

          totals.push(column.reduce(function(prev, current, index) {
            if (index === 1) {
              return current;
            }

            return prev + current;
          }));

        });
        // DO NOT change the format of this data without ensuring that whoever needs it is also changed!
        // Some examples: c3 charts, table widget.
        // $scope.chartData = {columns: columns, totals: totals, metricNames: metricNames, xCoords: xCoords};
        return {
          columns: columns,
          totals: totals,
          metricNames: metricNames,
          xCoords: xCoords
        };
      }
    }

    function convertDashboardToNewWidgets(dashboards) {
      if (angular.isArray(dashboards)) {
        dashboards.forEach(function(dashboard) {
          var widgets = [];
          dashboard.config.columns.forEach(function(column) {
            widgets = widgets.concat(column);
          });
          dashboard.config.columns = widgets;
          widgets.forEach(function(widget) {
            widget.settings = {};
            widget.settings.color = widget.color;
            widget.settings.isLive = widget.isLive;
            widget.settings.interval = widget.interval;
            widget.settings.aggregate = widget.aggregate;
          });
        });
      }
      return dashboards;
    }

    function formatTimeseries(aggregate, series, input, metric) {
      var processedData = processData(
        series,
        'qid',
        metric.names,
        metric.resolution
      );
      processedData = c3ifyData(processedData, metric, metric.names);

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

      return input;
    }

    return  {
      formatTimeseries: formatTimeseries,
      processData: processData,
      resolutionFromAuto: resolutionFromAuto,
      skipAmtFromResolution: skipAmtFromResolution,
      zeroFill: zeroFill,
      c3ifyData: c3ifyData,
      convertDashboardToNewWidgets: convertDashboardToNewWidgets
    };


  });
