angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceCreateController', function ($scope, $alert, MyDataSource) {
    $scope.model = {
      name: '',
      displayName: '',
      description: ''
    };
    $scope.socket = new MyDataSource($scope);
    $scope.submitHandler = function() {
      $scope.socket.post({
        config: {
          method: 'PUT',
          path: '/namespaces',
          isAbsoluteUrl: true,
          body: {
            name: $scope.model.name,
            displayName: $scope.model.displayName,
            descriotion: $scope.model.description
          }
        }
      },
      function(res) {
        console.log(arguments);
        $alert({
          title: 'Success!',
          content: 'Namespace Created!',
          type: 'success'
        });
      });
    };
  });
