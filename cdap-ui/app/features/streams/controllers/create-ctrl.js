angular.module(PKG.name + '.feature.streams')
  .controller('StreamsCreateController', function($scope, MyDataSource, $modalInstance, caskFocusManager) {

    caskFocusManager.focus('streamId');

    var dataSrc = new MyDataSource($scope);

    this.streamId = '';

    this.createStream = function() {
      dataSrc
        .request({
          _cdapNsPath: '/streams/' + $scope.streamId,
          method: 'PUT'
        })
        .then(function(res) {
          $modalInstance.close(res);
        }, function(err) {
          this.error = err;
        }.bind(this));
    };

    this.closeModal = function() {
      $modalInstance.close();
    };

  });
