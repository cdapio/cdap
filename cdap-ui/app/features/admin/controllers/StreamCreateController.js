angular.module(PKG.name + '.feature.admin')
  .controller('StreamsCreateController', function($scope, MyDataSource, $modalInstance, caskFocusManager, $stateParams) {

    caskFocusManager.focus('streamId');

    var dataSrc = new MyDataSource($scope);

    $scope.streamId = '';

    $scope.createStream = function() {
      dataSrc
        .request({
          _cdapPath: '/namespaces/' + $stateParams.nsadmin + '/streams/' + $scope.streamId,
          method: 'PUT'
        })
        .then(function(res) {
          $modalInstance.close(res);
        }, function(err) {
          $scope.error = err;
        });
    };

    $scope.closeModal = function() {
      $modalInstance.close();

    };

  });
