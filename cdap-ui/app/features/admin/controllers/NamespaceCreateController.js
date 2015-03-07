angular.module(PKG.name + '.feature.admin')
  .controller('AdminNamespaceCreateController', function ($scope, $alert, MyDataSource, myNamespace) {
    $scope.model = {
      name: '',
      description: ''
    };
    var myDataSrc = new MyDataSource($scope);
    $scope.submitHandler = function() {
      myDataSrc.request({
        method: 'PUT',
        _cdapPath: '/namespaces/' + $scope.model.id,
        body: {
          id: $scope.model.id,
          name: $scope.model.name || null,
          description: $scope.model.description
        }
      })
        .then(function(res) {
          $alert({
            title: 'Success!',
            content: 'Namespace Created!',
            type: 'success'
          });
          // Only place where we force fetch the namespace list
          // This is required as we need to update the list with the newly created namespace.
          myNamespace.getList(true);
        });
    };
  });
