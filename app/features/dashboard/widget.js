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

    return Widget;

  })

  .controller('WidgetCtrl', function ($scope, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    dataSrc.fetch({_cdap: 'GET '+$scope.wdgt.metric}, function (result) {
      $scope.chartHistory = [
        {
          label: $scope.wdgt.title,
          values: result.data.map(function (o) {
            return {
              time: o.time,
              y: o.value
            }
          })
        }
      ];
    });

  });
