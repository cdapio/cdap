angular.module(PKG.name + '.feature.admin')
  .controller('AdminNamespaceCreateController', function ($scope, $alert, $modalInstance, MyDataSource, myNamespace, EventPipe) {
    $scope.model = {
      name: '',
      description: ''
    };
    var myDataSrc = new MyDataSource($scope);
    $scope.submitHandler = _.once(function() {
      myDataSrc.request({
        method: 'PUT',
        _cdapPath: '/namespaces/' + $scope.model.name,
        body: {
          name: $scope.model.name,
          description: $scope.model.description
        }
      })
        .then(function() {
          $modalInstance.close();
          $alert({
            title: 'Success!',
            content: 'Namespace Created!',
            type: 'success'
          });

          myNamespace.getList(true).then(function() {
            EventPipe.emit('namespace.update');
          });
        });
    });
    $scope.closeModal = function() {
      $modalInstance.close();

    };
  });
