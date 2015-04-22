/**
 * Widget model & controllers
 */

angular.module(PKG.name+'.feature.dashboard')
  .factory('Widget', function (MyDataSource, MyMetrics) {

    function Widget (opts) {
      opts = opts || {};
      this.title = opts.title || 'Widget';
      this.type = opts.type;
      this.metric = opts.metric || false;
      this.color = opts.color;
      this.dataSrc = null;
      this.isLive = false;
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
        body: MyMetrics.constructQuery(queryId, MyMetrics.contextToTags(this.metric.context), this.metric.names)
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
          body: MyMetrics.constructQuery(queryId, MyMetrics.contextToTags(this.metric.context), this.metric.names)
        },
        this.processData.bind(this)
      );
    };

    Widget.prototype.stopPolling = function(id) {
      if (!this.dataSrc) return;
      this.dataSrc.stopPoll(id);
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
        tempMap[metric] = {};
        // interpolating the data since backend returns only metrics at specific time periods instead of
        // for the whole range. We have to interpolate the rest with 0s to draw the graph.
        for (j = result.startTime; j <= result.endTime; j++) {
          tempMap[metric][j] = 0;
        }
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
        tmpData.push(tempMap[metrics[i]]);
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

  .controller('WidgetTimeseriesCtrl', function ($scope, MyMetrics) {
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
        vs = MyMetrics.mapMetrics(newVal);

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
          hist.push({label: MyMetrics.cleanMetricName($scope.wdgt.metric.names[i]), values: vs[i]});
        }
        $scope.chartHistory = hist;
      }
    });

  })

  .controller('WidgetPieCtrl', function ($scope, MyMetrics) {
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
    // TODO: zero-filling is not needed for pie charts because we aggregate the values anyways
    $scope.wdgt.fetchData($scope);
    $scope.chartHistory = null;
    $scope.$watch('wdgt.data', function (newVal) {
      var metricMap, arr, vs, hist;
      if(angular.isObject(newVal)) {
        vs = MyMetrics.mapMetrics(newVal);

        var hasNonZero = function(dataPts) {
          for (var i = 0; i < dataPts.length; i++) {
            if (dataPts[i].value !== 0) {
              return true;
            }
          }
          return false;
        }
        hist = [];
        for (var i = 0; i < vs.length; i++) {
          hist.push({label: MyMetrics.cleanMetricName($scope.wdgt.metric.names[i]), value: MyMetrics.aggregate(vs[i])});
        }
        var hasValues = hasNonZero(hist);
        $scope.show = hasValues;
        if (hasValues) {
          $scope.chartHistory = hist;
        }
      }
    });
  });
