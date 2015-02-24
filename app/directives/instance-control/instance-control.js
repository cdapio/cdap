angular.module(PKG.name + '.commons')
  .directive('myInstanceControl', function ($alert, MyDataSource) {

    return {
      restrict: 'E',
      templateUrl: 'instance-control/instance-control.html',
      
      link: function (scope, element, attrs, controller) {

        scope.processing = false;

        scope.handleSet = function () {
          scope.processing = true;

          if (scope.instances.requested === undefined || scope.instances.requested < 0) {
            $alert({
              title: 'Invalid instances: ',
              content: 'you must request a valid number of instances.',
              type: 'danger'
            });
            scope.processing = false;
            return;
          }

          scope.myDataSrc.request({
            method: 'PUT',
            _cdapPathV2: scope.basePath + '/instances',
            data: {'instances': scope.instances.requested}
          }).then(function success (response) {
            scope.instances.provisioned = scope.instances.requested;
          }, function error (err) {
            //pass
          }).finally(function () {
            scope.processing = false;
          });

        }
      }
    };
  });

