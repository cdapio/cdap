/**
 * Widget model & controllers
 */

angular.module(PKG.name+'.feature.dashboard')
  .factory('Widget', function (MyDataSource) {

    function Widget (opts) {
      opts = opts || {};
      this.title = opts.title || 'Widget';
      this.type = opts.type;
      this.metric = opts.metric || false;
      this.color = opts.color;
      this.dataSrc = null;
      this.isLive = false;
    }

    // 'ns.default.app.foo' -> {'ns': 'default', 'app': 'foo'}
    function contextToTags(context) {
      parts = context.split('.');
      if (parts.length % 2 != 0) {
        throw "Metrics context has uneven number of parts: " + this.metric.context
      }
      tags = {};
      for (var i = 0; i < parts.length; i+=2) {
        tags[parts[i]] = parts[i + 1]
      }
      return tags;
    }

    function constructQuery(queryId, tags, metrics) {
      timeRange = {'start': 'now-60s', 'end': 'now'};
      queryObj = {tags: tags, metrics: metrics, groupBy: [], timeRange: timeRange};
      retObj = {};
      retObj[queryId] = queryObj;
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
        body: constructQuery(queryId, contextToTags(this.metric.context), this.metric.names)
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
          body: constructQuery(queryId, contextToTags(this.metric.context), this.metric.names)
        },
        this.processData.bind(this)
      );
    };

    Widget.prototype.stopPolling = function(id) {
      if (!this.dataSrc) return;
      this.dataSrc.stopPoll(id);
    };

    Widget.prototype.processData = function (queryResults) {
      var data, tempMap = {};
      result = queryResults[queryId];
      var metrics = this.metric.names;
      for (var i = 0; i < metrics.length; i++) {
        var metric = metrics[i];
        tempMap[metric] = {};
        // interpolating the data since backend returns only metrics at specific time periods instead of
        // for the whole range. We have to interpolate the rest with 0s to draw the graph.
        for (var j = result.startTime; j < result.endTime; j++) {
          tempMap[metric][j] = 0;
        }
      }
      for (var i = 0; i < result.series.length; i++) {
        var data = result.series[i].data;
        metric = result.series[i].metricName
        for (var k = 0 ; k < data.length; k++) {
          var dataPt = data[k];
          tempMap[metric][dataPt.time] = dataPt.value;
        }
      }
      tmpdata = [];
      for (var i = 0; i < metrics.length; i++) {
        tmpdata.push(tempMap[metrics[i]]);
      }
      this.data = tmpdata;
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
      if(angular.isObject(newVal)) {
        var vs = [];
        for (var i = 0; i < newVal.length; i++) {
          var metricMap = newVal[i];
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
            var lastIndex = el.length-1;
            arr.push(el[lastIndex])
          }
          $scope.stream = [arr]
        }

        hist = [];
        for (var i = 0; i < vs.length; i++) {
          hist.push({label: $scope.wdgt.metric.names[i], values: vs[i]});
        }
        $scope.chartHistory = hist;
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
