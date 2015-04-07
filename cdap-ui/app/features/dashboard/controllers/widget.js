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
    }

    Widget.prototype.fetchData = function (scope) {
      var dataSrc = new MyDataSource(scope);
      if(!this.metric) {
        return;
      }
      dataSrc.poll(
        {
          _cdapPath: '/metrics/query' +
            '?context=' + encodeURIComponent(this.metric.context) +
            '&metric=' + encodeURIComponent(this.metric.name) +
            '&start=now-60s&end=now',

          method: 'POST'
        },
        (function (result) {
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
        }).bind(this)
      );
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
