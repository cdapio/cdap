angular.module(PKG.name + '.commons')
  .directive('myInstanceControl', function (myAlert) {

    return {
      restrict: 'E',
      controller: 'instanceControlController',
      scope: {
        model: '=',
        basePath: '='
      },
      templateUrl: 'instance-control/instance-control.html',
      link: function (scope) {

        scope.processing = false;

        scope.handleSet = function () {
          scope.processing = true;

          if (scope.model.requested === undefined || scope.model.requested < 0) {
            myAlert({
              title: 'Invalid instances: ',
              content: 'you must request a valid number of instances.',
              type: 'danger'
            });
            scope.processing = false;
            return;
          }

          scope.myDataSrc.request({
            method: 'PUT',
            _cdapPath: scope.basePath + '/instances',
            body: {'instances': scope.model.requested}
          }).then(function success () {
            scope.model.provisioned = scope.model.requested;
          }).finally(function () {
            scope.processing = false;
          });

        };
      }
    };
  })
  .controller('instanceControlController', function ($scope, MyDataSource) {
    $scope.myDataSrc = new MyDataSource($scope);
  });
