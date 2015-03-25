angular.module(PKG.name + '.commons')
  .directive('loadingIcon', function(myLoadingService, $bootstrapModal, $timeout) {
    return {
      restrict: 'EA',
      scope: true,
      template: '<div></div>',
      link: function(scope, element, attrs) {
        var modalObj = {
          templateUrl: 'app-level-loading-icon/loading.html',
          size: 'lg',
          backdrop: 'static',
          keyboard: true,
          backdropClass: 'custom-login-backdrop'
        }, modal;
        scope.$on('hideLoadingIcon', function() {
          // Just making it smooth instead of being too 'speedy'
          $timeout(function() {
            modal && modal.close(true);
            modal = null;
          }, 2000);
        });

        scope.$on('showLoadingIcon', function() {
          if(!modal) {
            modal = $bootstrapModal.open(modalObj);
          }

        })
      }
    }
  });
