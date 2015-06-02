angular.module(PKG.name + '.commons')
  .directive('myInstanceControl', function (myAlert) {

    return {
      restrict: 'E',
      controller: 'instanceControlController',
      scope: {
        basePath: '='
      },
      templateUrl: 'instance-control/instance-control.html',
    };
  })
  .controller('instanceControlController', function ($scope, MyDataSource) {
    var myDataSrc = new MyDataSource($scope);

    myDataSrc.request({
      _cdapPath: $scope.basePath + '/instances'
    }).then(function (res) {
      $scope.instance = res;
    });

    $scope.handleSet = function () {
      myDataSrc.request({
        method: 'PUT',
        _cdapPath: $scope.basePath + '/instances',
        body: {'instances': $scope.instance.requested}
      }).then(function success () {
        $scope.instance.provisioned = $scope.instance.requested;
      });
    };

  });
