angular.module(PKG.name + '.commons')
  .directive('myInstanceControl', function (myAlert) {

    return {
      restrict: 'E',
      controller: 'instanceControlController',
      scope: {
        basePath: '='
      },
      templateUrl: 'instance-control/instance-control.html',
      link: function (scope) {

        scope.processing = false;

        scope.handleSet = function () {
          scope.processing = true;

          scope.myDataSrc.request({
            method: 'PUT',
            _cdapPath: scope.basePath + '/instances',
            body: {'instances': scope.instance.requested}
          }).then(function success (res) {
            scope.instance.provisioned = scope.instance.requested;
          }).finally(function () {
            scope.processing = false;
          });

        };
      }
    };
  })
  .controller('instanceControlController', function ($scope, MyDataSource) {
    $scope.myDataSrc = new MyDataSource($scope);

    $scope.myDataSrc.request({
      _cdapPath: $scope.basePath + '/instances'
    })
    .then(function (res) {
      $scope.instance = res;
    });
  });
