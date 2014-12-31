angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceCreateController', function ($scope, $alert, MyDataSource, myBaseUrl) {
    $scope.model = {
      name: '',
      displayName: '',
      description: ''
    };
    $scope.socket = new MyDataSource($scope);
    $scope.submitHandler = function() {
      $scope.socket.request({
        method: 'PUT',
        url: myBaseUrl +'/namespaces/',
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
