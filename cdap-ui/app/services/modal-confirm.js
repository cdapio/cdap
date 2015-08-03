angular.module(PKG.name+'.services')
  .factory('ModalConfirm', function($bootstrapModal) {

    // TODO: need to make this factory more modular

    function confirmModalAdapter (scope, plugin, copy, closeCb) {
      var watch = scope.$on('modal.closing', function (event, reason) {
        if ((reason === 'cancel' || reason === 'escape key press') && !scope.confirm ) {
          var stringCopy = JSON.stringify(copy);
          var stringPlugin = JSON.stringify(plugin);

          if (stringCopy !== stringPlugin) {
            event.preventDefault();

            var confirmInstance = $bootstrapModal.open({
              keyboard: false,
              templateUrl: '/assets/features/adapters/templates/partial/confirm.html',
              windowClass: 'modal-confirm',
              controller: ['$scope', function ($scope) {
                $scope.continue = function () {
                  $scope.$close('close');
                };

                $scope.cancel = function () {
                  $scope.$close('keep open');
                };
              }]
            });

            confirmInstance.result.then(function (closing) {
              if (closing === 'close') {
                scope.confirm = true;
                closeCb();
              }
            });
          }
        }
      });

      scope.$on('$destroy', function () {
        watch();
      });
    }

    return {
      confirmModalAdapter: confirmModalAdapter
    };

  });
