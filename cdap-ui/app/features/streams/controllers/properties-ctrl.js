angular.module(PKG.name + '.feature.streams')
  .controller('StreamPropertiesController', function($scope, MyDataSource, $modalInstance) {

    var dataSrc = new MyDataSource($scope);

    $scope.streamId = '';

    $scope.closeModal = function() {
      $modalInstance.close();

    };

  });