/**
 * Widget model & controller
 */

angular.module(PKG.name+'.feature.dashboard')
  .factory('Widget', function ($q) {

    function Widget (opts) {
      opts = opts || {};
      this.title = opts.title || 'Widget';
      this.partial = '/assets/features/dashboard/widgets/welcome.html';
    }

    Widget.prototype.getClassName = function () {
      return 'panel-default widget-'+this.partial.split('/').pop().split('.').shift();
    };

    return Widget;

  })

  .controller('WidgetTimeseriesCtrl', function ($scope, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    dataSrc.fetch({_cdap: 'GET '+$scope.wdgt.metric}, function (result) {
      $scope.chartHistory = [
        {
          label: $scope.wdgt.title,
          values: result.data.map(function (o) {
            return {
              x: o.time,
              y: o.value
            }
          })
        }
      ];
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
