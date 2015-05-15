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
      this.width = '';
      this.height = 200;
      this.dataSrc = null;
      this.isLive = opts.isLive || false;
      this.interval = opts.interval;
      this.aggregate = opts.aggregate;
      this.metricAlias =  opts.metricAlias || {};
      this.pollId = undefined;
    }

    function constructQuery(queryId, tags, metric) {
      var timeRange, retObj;
      timeRange = {'start': metric.startTime || 'now-60s',
                  'end': metric.endTime || 'now'};
      if (metric.resolution) {
        timeRange.resolution = metric.resolution;
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
        body: constructQuery(queryId, myHelpers.contextToTags(this.metric.context), this.metric)
      })
      .then(this.processData.bind(this));
    };

    Widget.prototype.reconfigure = function (scope) {
      // stop any polling (if any was in session)
      this.stopPolling();
      if (this.isLive) {
        this.startPolling(scope);
      } else {
        this.fetchData(scope);
      }
    };

    Widget.prototype.startPolling = function (scope) {
      if (!this.dataSrc) {
        this.dataSrc = new MyDataSource(scope);
      }
      if(!this.metric) {
        return;
      }
      if (this.pollId !== undefined) {
        // already polling
        return;
      }
      this.dataSrc.poll(
        {
          _cdapPath: '/metrics/query',
          method: 'POST',
          body: constructQuery(queryId, myHelpers.contextToTags(this.metric.context), this.metric),
          interval: this.interval
        }, this.processData.bind(this));
        // Should work just the same with promises too.
        // .then(this.processData.bind(this));
    };

    Widget.prototype.stopPolling = function() {
      if (!this.dataSrc) return;
      if (this.pollId === undefined) {
        // not currently polling
        return;
      }
      this.dataSrc.stopPoll(this.pollId);
      this.pollId = undefined;
    };

    // Compute resolution since back-end doesn't provide us the resolution when 'auto' is used
    var resolutionFromAuto = function(startTime, endTime) {
      var diff = endTime - startTime;
      if (diff <= 600) {
        return '1s';
      } else if (diff <= 36000) {
        return '1m';
      }
      return '1h';
    };

    var skipAmtFromResolution = function(resolution) {
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
    var zeroFill = function(resolution, result) {
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

    Widget.prototype.processData = function (queryResults) {
      this.pollId = queryResults.__pollId__;
      var metrics, metric, data, dataPt, result;
      var i, j;
      var tempMap = {};
      var tmpData = [];
      result = queryResults[queryId];
      metrics = this.metric.names;
      for (i = 0; i < metrics.length; i++) {
        metric = metrics[i];
        tempMap[metric] = zeroFill(this.metric.resolution, result);
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

    Widget.prototype.getClassName = function (last) {

      var className = 'panel-default widget widget-' + this.type;
      if (last) {
        className = className + ' last-item';
      }
      return className;
    };

    return Widget;

  })

  .controller('DropdownCtrl', function ($scope, $state, $dropdown) {
    $scope.ddWidget = function(event){
      var toggle = angular.element(event.target);
      if(!toggle.hasClass('dropdown-toggle')) {
        toggle = toggle.parent();
      }

      var scope = $scope.$new(),
          dd = $dropdown(toggle, {
            template: 'assets/features/dashboard/templates/partials/wdgt-dd.html',
            animation: 'am-flip-x',
            trigger: 'manual',
            prefixEvent: 'wdgt-tab-dd',
            scope: scope
          });

      dd.$promise.then(function(){
        dd.show();
      });

      scope.$on('wdgt-tab-dd.hide', function () {
        dd.destroy();
      });
    };
  })

  .controller('WidgetColCtrl', function ($scope) {
    $scope.colWidth = {
      fullWidth: false,
      oneThird: true
    };
  })

  .controller('WidgetTimeseriesCtrl', function ($scope) {
    $scope.wdgt.reconfigure($scope);
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
          hist.push({label: replacedMetricName, values: vs[i]});
        }
        $scope.chartHistory = hist;
      }
    });

  })

 .controller('C3WidgetTimeseriesCtrl', function ($scope, myHelpers, $timeout) {
    $scope.chartData = null;
    $scope.chartSize = { height: 200 };
    var widget = myHelpers.objectQuery($scope, 'gridsterItem', '$element', 0),
        widgetHeight;
    if (widget) {
      widgetHeight = parseInt(widget.style.height, 10);
      widgetWidth = parseInt(widget.style.width, 10);
      if (widgetHeight > 300) {
        $scope.wdgt.height = widgetHeight - 70;
      }
      $scope.wdgt.width = widgetWidth - 32;
    }

    $scope.wdgt.reconfigure($scope);

    $scope.$on('gridster-resized', function(event, sizes) {
      $timeout(function() {
        $scope.chartSize.height = parseInt($scope.gridsterItem.$element[0].style.height, 10) - 70;
        $scope.chartSize.width = parseInt($scope.gridsterItem.$element[0].style.width, 10) - 32;
      });
    });

    $scope.$watch('wdgt.height', function(newVal) {
      $scope.chartSize.height = newVal;
    });
    $scope.$watch('wdgt.width', function(newVal) {
      if (!newVal) {
        return;
      }
      $scope.chartSize.width = newVal;
    });
    $scope.$watch('wdgt.data', function (newVal) {
      var metricMap, columns, streams;
      if(angular.isObject(newVal) && newVal.length) {

        var metricNames = $scope.wdgt.metric.names.map(function(metricName) {
          var metricAlias = $scope.wdgt.metricAlias[metricName];
          if (metricAlias !== undefined) {
            metricName = metricAlias;
          }
          return metricName;
        });


        // columns will be in the format: [ [metric1Name, v1, v2, v3, v4], [metric2Name, v1, v2, v3, v4], ... xCoords ]
        columns = [];
        for (var i = 0; i < newVal.length; i++) {
          metricMap = newVal[i];
          var values = Object.keys(metricMap).map(function(key) {
            return metricMap[key];
          });
          values.unshift(metricNames[i]);
          columns.push(values);
        }

        // x coordinates are expected in the format: ['x', ts1, ts2, ts3...]
        var xCoords = Object.keys(newVal[0]);
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
        $scope.chartData = {columns: columns, streams: streams, metricNames: metricNames, xCoords: xCoords};
      }
    });

  })

  .controller('WidgetTableCtrl', function ($scope) {
    $scope.$watch('chartData', function (chartData) {
      if (!chartData) {
        return;
      }
      var tableData = [];
      chartData.xCoords.forEach(function(timestamp, index) {
        if (index === 0) {
          // the first index of each column is just 'x' or the metric name
          return;
        }
        var rowData = [timestamp];
        chartData.columns.forEach(function(column) {
          // If it begins with 'x', it is timestamps
          if (column.length && column[0] !== 'x') {
            rowData.push(column[index]);
          }
        });
        tableData.push(rowData);
      });
      $scope.tableData = tableData;
    });
  })

  .controller('WidgetPieCtrl', function ($scope, $alert) {

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
