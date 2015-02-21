angular.module(PKG.name + '.commons')
  .directive('myInstanceControl', function ($alert, MyDataSource) {

    return {
      restrict: 'E',

      scope: {
        model: '=',
        basePath: '='
      },

      templateUrl: 'instance-control/instance-control.html',

      link: function (scope, element, attrs) {
        var dataSrc = new MyDataSource(scope)


        scope.processing = false;

        scope.handleSet = function () {
          scope.processing = true;

          if (scope.model.requested === undefined || scope.model.requested < 0) {
            $alert({
              title: 'Invalid instances: ',
              content: 'you must request a valid number of instances.',
              type: 'danger'
            });
            scope.processing = false;
            return;
          }

          dataSrc.request({
            method: 'PUT',
            _cdapPathV2: scope.basePath + '/instances',
            data: {'instances': scope.model.requested}
          }).then(function success (response) {
            scope.model.provisioned = scope.model.requested;
          }).finally(function () {
            scope.processing = false;
          });

        }
      }
    };
  });

