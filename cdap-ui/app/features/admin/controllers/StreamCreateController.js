angular.module(PKG.name + '.feature.admin')
  .controller('StreamsCreateController', function($scope, $modalInstance, caskFocusManager, $stateParams, myStreamApi) {

    caskFocusManager.focus('streamId');

    $scope.streamId = '';

    $scope.createStream = function() {
      var params = {
        namespace: $stateParams.nsadmin,
        streamId: $scope.streamId,
        scope: $scope
      };
      myStreamApi.create(params, {})
        .$promise
        .then(function(res) {
          $modalInstance.close(res);
        }, function (err) {
          $scope.error = err;
        });
    };

    $scope.closeModal = function() {
      $modalInstance.close();
    };

  });
