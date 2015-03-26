angular.module(PKG.name + '.commons')
  .directive('loadingIcon', function(myLoadingService, $bootstrapModal, $timeout, EventPipe, $state, $alert) {
    return {
      restrict: 'EA',
      scope: true,
      template: '<div></div>',
      controller: function($scope) {
        var modalObj = {
          templateUrl: 'app-level-loading-icon/loading.html',
          size: 'lg',
          backdrop: 'static',
          keyboard: true,
          scope: $scope,
          windowClass: 'custom-loading-modal'
        }, modal, isBackendDown = false;

        EventPipe.on('backendDown', function() {
          if (!isBackendDown) {
            modal && modal.close();
            isBackendDown = true;
            $scope.message = 'Waiting for CDAP services to be online...';
            modal = $bootstrapModal.open(modalObj);
            modal.result.finally(function() {
              $state.go('overview', {}, {reload: true});
            });
          }
        }.bind($scope));

        EventPipe.on('backendUp', function() {
          if (isBackendDown) {
            modal.close();
            modal = null;
            isBackendDown = false;
            $alert({
              title: 'We\'re Back!',
              type: 'success',
              content: 'CDAP Services are back online'
            });
          }
        }.bind($scope));

        EventPipe.on('hideLoadingIcon', function() {
          // Just making it smooth instead of being too 'speedy'
          if (!isBackendDown) {
            $timeout(function() {
              modal && !modal.$state && modal.close();
              modal = null;
              isLoading = false;
            }, 2000);
          }
        });

        EventPipe.on('showLoadingIcon', function() {
          if(!modal && !isBackendDown) {
            $scope.message = 'Loading the Application... ';
            modal = $bootstrapModal.open(modalObj);
          }
        }.bind($scope));
      }
    }
  });
