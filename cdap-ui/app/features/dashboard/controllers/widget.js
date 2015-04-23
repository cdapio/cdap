/**
 * Widget model & controllers
 */

angular.module(PKG.name+'.feature.dashboard')
  .factory('Widget', function (MyDataSource, myHelpers) {

    function Widget (opts) {
      opts = opts || {};
      this.title = opts.title || 'Widget';
      this.type = opts.type;
      this.metric = opts.metric || false;
      // TODO: reconsider this field once Epoch is removed.
      this.color = opts.color;
      this.dataSrc = null;
      this.isLive = opts.isLive || false;
      this.interval = opts.interval;
      this.aggregate = opts.aggregate;
      this.metricAlias =  opts.metricAlias || {};
    }

    // 'ns.default.app.foo' -> {'ns': 'default', 'app': 'foo'}
    function contextToTags(context) {
      var parts, tags, i;
      parts = context.split('.');
      if (parts.length % 2 != 0) {
        throw "Metrics context has uneven number of parts: " + this.metric.context
      }
      tags = {};
      for (i = 0; i < parts.length; i+=2) {
        // In context, '~' is used to represent '.'
        var tagValue = parts[i + 1].replace(/~/g, '.');
        tags[parts[i]] = tagValue;
      }
      return tags;
    }

    function constructQuery(queryId, tags, metric) {
      var timeRange, retObj;
      timeRange = {'start': metric.startTime || 'now-60s',
                  'end': metric.endTime || 'now'};
      if (metric.resolution) {
        timeRange['resolution'] = metric.resolution;
      }
      retObj = {};
      retObj[queryId] = {tags: tags, metrics: metric.names, groupBy: [], timeRange: timeRange};
      return retObj;
    }

    // The queryId value does not matter, as long as we are using the same value in the request
    // as in parsing the response.
    var queryId = "qid";
    Widget.prototype.fetchData = function(scope) {
      if (!this.dataSrc) {
        this.dataSrc = new MyDataSource(scope);
      }
      this.dataSrc.request({
        _cdapPath: '/metrics/query',
        method: 'POST',
        body: constructQuery(queryId, contextToTags(this.metric.context), this.metric)
      })
        .then(this.processData.bind(this))
    }
    Widget.prototype.startPolling = function (scope) {
      if (!this.dataSrc) {
        this.dataSrc = new MyDataSource(scope);
      }
      if(!this.metric) {
        return;
      }
      return this.dataSrc.poll(
        {
          _cdapPath: '/metrics/query',
          method: 'POST',
          body: constructQuery(queryId, contextToTags(this.metric.context), this.metric),
          interval: this.interval
        },
        this.processData.bind(this)
      );
    };

    Widget.prototype.stopPolling = function(id) {
      if (!this.dataSrc) return;
      this.dataSrc.stopPoll(id);
    };

    var zeroFill = function(resolution, result) {
        // interpolating (filling with zeros) the data since backend returns only metrics at specific time periods
        // instead of for the whole range. We have to interpolate the rest with 0s to draw the graph.
        if (resolution == '1h') {
          skipAmt = 60 * 60;
        } else if (resolution == '1m') {
          skipAmt = 60;
        } else {
          skipAmt = 1;
        }

        var startTime = myHelpers.roundUpToNearest(result.startTime, skipAmt);
        var endTime = myHelpers.roundDownToNearest(result.endTime, skipAmt);
        var tempMap = {};
        for (j = startTime; j <= endTime; j += skipAmt) {
          tempMap[j] = 0;
        }
        return tempMap;
    };

    Widget.prototype.processData = function (queryResults) {
      var metrics, metric, data, dataPt, result;
      var i, j;
      var tempMap = {}
      var tmpData = [];
      result = queryResults[queryId];
      metrics = this.metric.names;
      for (i = 0; i < metrics.length; i++) {
        metric = metrics[i];
        tempMap[metric] = zeroFill(this.metric.resolution, result);
      }
      for (i = 0; i < result.series.length; i++) {
        data = result.series[i].data;
        metric = result.series[i].metricName
        for (j = 0 ; j < data.length; j++) {
          dataPt = data[j];
          tempMap[metric][dataPt.time] = dataPt.value;
        }
      }
      for (i = 0; i < metrics.length; i++) {
        var thisMetricData = tempMap[metrics[i]];
        if (this.aggregate) {
          thisMetricData = myHelpers.aggregate(thisMetricData, this.aggregate);
        }
        tmpData.push(thisMetricData);
      }
      this.data = tmpData;
    };

    Widget.prototype.getPartial = function () {
      return '/assets/features/dashboard/templates/widgets/' + this.type + '.html';
    };

    Widget.prototype.getClassName = function () {
      return 'panel-default widget widget-' + this.type;
    };

    return Widget;

  })

  .controller('WidgetColCtrl', function ($scope) {
    $scope.colWidth = {
      fullWidth: false,
      oneThird: true
    };
  })

  .controller('WidgetTimeseriesCtrl', function ($scope) {
    var pollingId = null;
    $scope.$watch('wdgt.isLive', function(newVal) {
      if (!angular.isDefined(newVal)) {
        return;
      }
      if (newVal) {
        pollingId = $scope.wdgt.startPolling();
      } else {
        $scope.wdgt.stopPolling(pollingId);
      }
    });
    $scope.wdgt.fetchData($scope);
    $scope.chartHistory = null;
    $scope.stream = null;
    $scope.$watch('wdgt.data', function (newVal) {
      var metricMap, arr, vs, hist;
      if(angular.isObject(newVal)) {
        vs = [];
        for (var i = 0; i < newVal.length; i++) {
          metricMap = newVal[i];
          vs.push(Object.keys(metricMap).map(function(key) {
            return {
              time: key,
              y: metricMap[key]
            };
          }));
        }

        if ($scope.chartHistory) {
          arr = [];
          for (var i = 0; i < vs.length; i++) {
            var el = vs[i];
            var lastIndex = el.length - 1;
            arr.push(el[lastIndex]);
          }
          $scope.stream = arr;
        }

        hist = [];
        for (var i = 0; i < vs.length; i++) {
          // http://stackoverflow.com/questions/20306204/using-queryselector-with-ids-that-are-numbers
          // http://www.w3.org/TR/CSS21/syndata.html#value-def-identifier
          var metricName = $scope.wdgt.metric.names[i];

          var metricAlias = $scope.wdgt.metricAlias[metricName];
          if (metricAlias !== undefined) {
            metricName = metricAlias;
          }
          // Replace all invalid characters with '_'. This is ok for now, since we do not display the chart labels
          // to the user. Source: http://stackoverflow.com/questions/13979323/how-to-test-if-selector-is-valid-in-jquery
          var replacedMetricName = metricName.replace(/([;&,\.\+\*\~':"\!\^#$%@\[\]\(\)=><\|])/g, '_');
          hist.push({label: metricName, values: vs[i]});
        }
        $scope.chartHistory = hist;
      }
    });

  })

 .controller('C3WidgetTimeseriesCtrl', function ($scope) {
    var pollingId = null;
    $scope.$watch('wdgt.isLive', function(newVal) {
      if (!angular.isDefined(newVal)) {
        return;
      }
      if (newVal) {
        pollingId = $scope.wdgt.startPolling();
      } else {
        $scope.wdgt.stopPolling(pollingId);
      }
    });
    $scope.wdgt.fetchData($scope);
    $scope.chartHistory = null;
    $scope.$watch('wdgt.data', function (newVal) {
      var metricMap, arr, columns, hist;
      if(angular.isObject(newVal) && newVal.length) {
        // columns will be in the format: [ [metric1Name, v1, v2, v3, v4], [metric2Name, v1, v2, v3, v4], ... xCoords ]
        columns = [];
        for (var i = 0; i < newVal.length; i++) {
          metricMap = newVal[i];
          var values = Object.keys(metricMap).map(function(key) {
            return metricMap[key];
          });
          values.unshift($scope.wdgt.metric.names[i]);
          columns.push(values);
        }

        // x coordinates are expected in the format: ['x', ts1, ts2, ts3...]
        var xCoords = Object.keys(newVal[0]);
        xCoords.unshift('x');
        columns.push(xCoords);

        var metricNames = $scope.wdgt.metric.names.map(function(metricName) {
          var metricAlias = $scope.wdgt.metricAlias[metricName];
          if (metricAlias !== undefined) {
            metricName = metricAlias;
          }
          return metricName;
        });
        $scope.chartData = {columns: columns, metricNames: metricNames};
      }
    });

  })

  .controller('WidgetPieCtrl', function ($scope, $alert, MyDataSource) {

    $alert({
      content: 'pie chart using fake data',
      type: 'warning'
    });

    $scope.pieChartData = [
      { label: 'Slice 1', value: 10 },
      { label: 'Slice 2', value: 20 },
      { label: 'Slice 3', value: 40 },
      { label: 'Slice 4', value: 30 }
    ];

  });
