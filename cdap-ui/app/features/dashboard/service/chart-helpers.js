angular.module(PKG.name + '.feature.dashboard')
  .factory('MyChartHelpers', function(myHelpers) {

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
          thisMetricData = myHelpers.aggregate(thisMetricData, isAggregate);
        }
        tmpData.push(thisMetricData);
      }
      // this.data = tmpData;
      return tmpData;
    };

    // Compute resolution since back-end doesn't provide us the resolution when 'auto' is used
    function resolutionFromAuto(startTime, endTime) {
      var diff = endTime - startTime;
      if (diff <= 600) {
        return '1s';
      } else if (diff <= 36000) {
        return '1m';
      }
      return '1h';
    };
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
    };
    function zeroFill(resolution, result) {
        // interpolating (filling with zeros) the data since backend returns only metrics at specific time periods
        // instead of for the whole range. We have to interpolate the rest with 0s to draw the graph.
        if (resolution === 'auto') {
          resolution = resolutionFromAuto(result.startTime, result.endTime);
        }
        var skipAmt = skipAmtFromResolution(resolution);

        var startTime = myHelpers.roundUpToNearest(result.startTime, skipAmt);
        var endTime = myHelpers.roundDownToNearest(result.endTime, skipAmt);
        var tempMap = {};
        for (var j = startTime; j <= endTime; j += skipAmt) {
          tempMap[j] = 0;
        }
        return tempMap;
    };

    function c3ifyData (newVal, metrics, alias) {
      var metricMap,
          columns,
          streams,
          metricNames,
          metricAlias,
          i,
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
        for (i = 0; i < newVal.length; i++) {
          metricMap = newVal[i];
          values = Object.keys(metricMap).map(function(key) {
            return metricMap[key];
          });
          values.unshift(metricNames[i]);
          columns.push(values);
        }

        // x coordinates are expected in the format: ['x', ts1, ts2, ts3...]
        xCoords = Object.keys(newVal[0]);
        xCoords.unshift('x');
        columns.push(xCoords);

        streams = [];
        columns.forEach(function(column) {
          if (!column.length || column[0] === 'x') {
            return;
          }
          streams.push(column[column.length - 1]);
        });
        // DO NOT change the format of this data without ensuring that whoever needs it is also changed!
        // Some examples: c3 charts, table widget.
        // $scope.chartData = {columns: columns, streams: streams, metricNames: metricNames, xCoords: xCoords};
        return {
          columns: columns,
          streams: streams,
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

    return  {
      processData: processData,
      resolutionFromAuto: resolutionFromAuto,
      skipAmtFromResolution: skipAmtFromResolution,
      zeroFill: zeroFill,
      c3ifyData: c3ifyData,
      convertDashboardToNewWidgets: convertDashboardToNewWidgets
    }


  })

  .factory('MyMetricsQueryHelper', function() {
    // 'ns.default.app.foo' -> {'ns': 'default', 'app': 'foo'}
    function contextToTags(context) {
      var parts, tags, i, tagValue;
      if (context.length) {
        parts = context.split('.');
      } else {
        // For an empty context, we want no tags. Splitting it by '.' yields [""]
        parts = [];
      }
      if (parts.length % 2 !== 0) {
        throw "Metrics context has uneven number of parts: " + context;
      }
      tags = {};
      for (i = 0; i < parts.length; i+=2) {
        // In context, '~' is used to represent '.'
        tagValue = parts[i + 1].replace(/~/g, '.');
        tags[parts[i]] = tagValue;
      }
      return tags;
    }

    // TODO: Need to figure out a way to pass url for a chart
    // that is part of the widget, which is not a metric.
    // Right now a chart and a metric is tied together and
    // it needs to be changed.
    function constructQuery(queryId, tags, metric) {
      var timeRange, retObj;
      timeRange = {
        'start': metric.startTime || 'now-60s',
        'end': metric.endTime || 'now'
      };
      if (metric.resolution) {
        timeRange.resolution = metric.resolution;
      }
      retObj = {};
      retObj[queryId] = {
        tags: tags,
        metrics: metric.names,
        groupBy: [],
        timeRange: timeRange
      };
      return retObj;
    }


    return {
      contextToTags: contextToTags,
      constructQuery: constructQuery
    };
  });
