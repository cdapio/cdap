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
      this.startTime = 'now-60s';
      this.endTime = 'now';
      this.color = opts.color;
      this.dataSrc = null;
      this.isLive = false;
      this.pollingId = null;
    }

    Widget.prototype.fetchData = function(scope, startMs, endMs) {
      if (!this.dataSrc) {
        this.dataSrc = new MyDataSource(scope);
      }
      var cdapPath = '/metrics/query' +
        '?context=' + encodeURIComponent(this.metric.context) +
        '&metric=' + encodeURIComponent(this.metric.name) +
        '&start=' + (startMs? (startMs/1000): 'now-60s') +
        '&end=' + (endMs? (endMs/1000): 'now');

      this.dataSrc.request({
        _cdapPath: cdapPath,
        method: 'POST'
      })
      .then(this.processData.bind(this));
    };

    Widget.prototype.fetchDuration = function(scope, durationMs) {
      if (!this.dataSrc) {
        this.dataSrc = new MyDataSource(scope);
      }
      this.startTime = 'now-' + durationMs / 1000 + 's';
      var cdapPath = '/metrics/query' +
      '?context=' + encodeURIComponent(this.metric.context) +
      '&metric=' + encodeURIComponent(this.metric.name) +
      '&start=' + this.startTime +
      '&end=' + this.endTime;

      this.dataSrc.request({
        _cdapPath: cdapPath,
        method: 'POST'
      })
      .then(this.processData.bind(this));
    };

    Widget.prototype.resetPollingFrequency = function(scope, frequency) {
      if (!this.dataSrc) {
        this.dataSrc = new MyDataSource(scope);
      }
      this.stopPolling(this.pollingId);
    };

    Widget.prototype.startPolling = function (scope, frequency) {
      if (!this.dataSrc) {
        this.dataSrc = new MyDataSource(scope);
      }
      if(!this.metric) {
        return;
      }
      var resourceObj = {
        _cdapPath: '/metrics/query' +
          '?context=' + encodeURIComponent(this.metric.context) +
          '&metric=' + encodeURIComponent(this.metric.name) +
          '&start=' + this.startTime +
          '&end=now',

        method: 'POST'
      };
      if(frequency) {
        resourceObj.frequency = frequency;
      }
      this.pollingId = this.dataSrc.poll(resourceObj, this.processData.bind(this));
      return this.pollingId;
    };

    Widget.prototype.stopPolling = function(id) {
      if (!this.dataSrc) return;
      this.dataSrc.stopPoll(id);
    };

    Widget.prototype.processData = function (result) {
      var data, tempMap = {};
      if(result.series && result.series.length) {
        data = result.series[0].data;
        for (var k =0 ; k<data.length; k++) {
          tempMap[data[k].time] = data[k].value;
        }
      }
      // interpolating the data since backend returns only
      // metrics at specific timeperiods instead of for the
      // whole range. We have to interpolate the rest with 0s to draw the graph.
      for(var i = result.startTime; i<result.endTime; i++) {
        if (!tempMap[i]) {
          tempMap[i] = 0;
        }
      }
      this.data = tempMap;
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
    $scope.parsedRefreshRate = 1000;
    var pollingId = null;
    
    $scope.$watch('wdgt.isLive', function(newVal) {
      if (!angular.isDefined(newVal)) {
        return;
      }
      if (newVal) {
        pollingId = $scope.wdgt.startPolling($scope, $scope.parsedRefreshRate);
      } else {
        $scope.wdgt.stopPolling(pollingId);
      }
    });
    
    $scope.$watch('startMs', function(newVal) {
      if (!newVal) {
        return;
      }
      $scope.wdgt.isLive = false;
      $scope.wdgt.fetchData($scope, newVal, $scope.wdgt.endMs);
      console.log('StartMs: ', newVal);
    });
    
    $scope.$watch('endMs', function(newVal) {
      if (!newVal) {
        return;
      }
      $scope.wdgt.isLive = false;
      $scope.wdgt.fetchData($scope, $scope.wdgt.startMs, newVal);
      console.log('EndMs: ', newVal);
    });
    
    $scope.$watch('durationMs', function (newVal) {
      if (!newVal) {
        return;
      }
      $scope.wdgt.isLive = false;
      $scope.wdgt.fetchDuration($scope, $scope.wdgt.durationMs);
      console.log('DurationMs: ', newVal);
    });

    $scope.$watch('refreshRate', function (newVal, oldVal) {
      if (!newVal || newVal === oldVal) {
        return;
      }
      $scope.wdgt.isLive = false;
      $scope.parsedRefreshRate = parseRefreshRate(newVal);
      $scope.wdgt.resetPollingFrequency($scope, $scope.parsedRefreshRate);
      console.log('refreshRate: ', newVal);
    });

    $scope.wdgt.fetchData($scope);
    $scope.chartHistory = null;
    $scope.stream = null;
    
    $scope.$watch('wdgt.data', function (newVal) {
      var v;
      if(angular.isObject(newVal)) {
        v = Object.keys(newVal).map(function(key) {
          return {
            time: key,
            y: newVal[key]
          };
        });

        if ($scope.chartHistory) {
          $scope.stream = v.slice(-1);
        }

        $scope.chartHistory = [
          {
            label: $scope.wdgt.metric.name,
            values: v
          }
        ];

      }
    });

    function parseRefreshRate(val) {
      switch(val) {
        case '1 second':
          return 1000;
        case '60 seconds':
          return 1000 * 60;
        case '10 mins':
          return 1000 * 60 * 10;
        case '1 hour':
          return 1000 * 60 * 60;
      }
    }
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
