/**
 * Widget model & controllers
 */

angular.module(PKG.name+'.feature.dashboard')
  .factory('Widget', function (MyDataSource) {

    var dataSrc = new MyDataSource();

    function Widget (opts) {
      opts = opts || {};
      this.title = opts.title || 'Widget';
      this.type = opts.type || 'welcome';
      this.metric = opts.metric || false;
    }

    Widget.prototype.fetchData = function () {
      if(!this.metric) {
        return;
      }
      dataSrc.request(
        {
          _cdapPath: '/metrics/query' +
            '?context=' + encodeURIComponent(this.metric.context) +
            '&metric=' + encodeURIComponent(this.metric.name) +
            '&start=now-60s&end=now',

          method: 'POST'
        },
        (function (result) {
          if(result.series && result.series.length) {
            var data = result.series[0];
            data.splice(data.length-1, 1);
            this.data = data;
          }
        }).bind(this)
      );
    };


    Widget.prototype.getPartial = function () {
      return '/assets/features/dashboard/widgets/' + this.type + '.html';
    };

    Widget.prototype.getClassName = function () {
      return 'panel-default widget widget-' + this.type;
    };

    return Widget;

  })

  .controller('WidgetTimeseriesCtrl', function ($scope) {

    $scope.wdgt.fetchData();

    $scope.$watch('wdgt.data', function (newVal) {
      if(angular.isArray(newVal)) {

        $scope.chartHistory = [
          {
            label: $scope.wdgt.metric.name,
            values: newVal.map(function (o) {
              return {
                time: o.time,
                y: o.value
              };
            })
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
