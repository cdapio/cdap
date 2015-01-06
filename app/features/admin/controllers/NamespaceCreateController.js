angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceCreateController', function ($scope, $alert, MyDataSource) {
    $scope.model = {
      name: '',
      displayName: '',
      description: ''
    };
    var myDataSrc = new MyDataSource($scope);
    $scope.submitHandler = function() {
      myDataSrc.request({
        method: 'PUT',
        _cdapPath: '/namespaces/' + $scope.model.id,
        body: {
          id: $scope.model.id,
          displayName: $scope.model.displayName,
          description: $scope.model.description
        }
      },
      function(res) {
        $alert({
          title: 'Success!',
          content: 'Namespace Created!',
          type: 'success'
        });
      });
    };
  });
