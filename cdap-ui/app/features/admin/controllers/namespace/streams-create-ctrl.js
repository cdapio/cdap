angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceStreamsCreateController', function($scope, $modalInstance, caskFocusManager, $stateParams, myStreamApi, $alert, $timeout) {

    caskFocusManager.focus('streamId');

    $scope.isSaving = false;
    $scope.streamId = '';

    $scope.createStream = function() {
      $scope.isSaving = true;
      var params = {
        namespace: $stateParams.nsadmin,
        streamId: $scope.streamId,
        scope: $scope
      };
      myStreamApi.create(params, {})
        .$promise
        .then(function(res) {
          // FIXME: We reload on state leave. So when we show an alert
          // here and reload the state we have that flickering in the
          // alert message. In order to avoid that I am delaying by 100ms
          $timeout(function() {
            $alert({
              type: 'success',
              content: 'Stream ' + $scope.streamId + ' created successfully'
            });
          }, 100);
          $scope.isSaving = false;
          $modalInstance.close(res);
        }, function (err) {
          $scope.isSaving = false;
          $scope.error = err.data;
        });
    };

    $scope.closeModal = function() {
      $modalInstance.close();
    };

  });
