angular.module(PKG.name + '.feature.admin')
  .controller('AdminNamespaceCreateController', function ($scope, $alert, $modalInstance, MyDataSource, myNamespace) {
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
          // Only place where we force fetch the namespace list
          // This is required as we need to update the list with the newly created namespace.
          myNamespace.getList(true);
        });
    });
    $scope.closeModal = function() {
      $modalInstance.close();

    };
  });
