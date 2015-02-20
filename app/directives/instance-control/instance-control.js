angular.module(PKG.name + '.commons')
  .directive('myInstanceControl', function ($timeout, MyDataSource) {

    return {
      restrict: 'E',

      scope: {
        model: '=',
        basepath: '='
      },

      templateUrl: 'instance-control/instance-control.html',

      link: function (scope, element, attrs) {
        var dataSrc = new MyDataSource(scope)


        scope.processing = false;

        scope.handleSet = function () {
          scope.processing = true;

          dataSrc.request({
            method: 'PUT',
            _cdapPathV2: scope.basepath + '/instances',
            data: {'instances': scope.model.requested}
          }).then(function success (response) {
            console.log('here')
            scope.model.provisioned = scope.model.requested;
            scope.processing = false;
          }, function error () {
            console.log (arguments);
          });

        }
      }
    };
  });

