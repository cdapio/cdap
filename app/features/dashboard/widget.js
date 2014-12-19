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

    console.log($scope.wdgt);

    dataSrc.fetch({_cdap: 'GET '+$scope.wdgt.metric}, function (result) {
      console.log(result);
      $scope.chartHistory = result;
    });

  });
