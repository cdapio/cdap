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
        _cdapPath: '/namespaces/' + $scope.model.displayName,
        body: {
          name: $scope.model.name,
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
