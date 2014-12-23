angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceCreateController', function ($scope, $alert, MyDataSource, MY_CONFIG) {
    $scope.model = {
      name: '',
      displayName: '',
      description: ''
    };
    $scope.socket = new MyDataSource($scope);
    $scope.submitHandler = function() {
      $scope.socket.request({
        method: 'PUT',
        url: 'http://' +
          MY_CONFIG.cdap.routerServerUrl +
          ':' +
          MY_CONFIG.cdap.routerServerPort +
          '/v3/namespaces/',
        body: {
          name: $scope.model.name,
          displayName: $scope.model.displayName,
          descriotion: $scope.model.description
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
