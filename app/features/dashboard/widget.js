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

      var m = opts.metric;
      if(m) {
        this.metric = m;

        dataSrc.request(
          {
            _cdapPath: m,
            method: 'POST'
          },
          (function (result) {
            this.data = result.data;
          }).bind(this)
        );
      }
    }

    Widget.prototype.getPartial = function () {
      return '/assets/features/dashboard/widgets/' + this.type + '.html';
    };

    Widget.prototype.getClassName = function () {
      return 'panel-default widget widget-' + this.type;
    };

    return Widget;

  })

  .controller('WidgetTimeseriesCtrl', function ($scope) {

    $scope.$watch('wdgt.data', function (newVal) {
      if(angular.isArray(newVal)) {

        $scope.chartHistory = [
          {
            label: $scope.wdgt.metric,
            values: newVal.map(function (o) {
              return {
                x: o.time,
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
