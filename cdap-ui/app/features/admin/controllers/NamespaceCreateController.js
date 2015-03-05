angular.module(PKG.name + '.feature.admin')
  .controller('AdminNamespaceCreateController', function ($scope, $alert, myNamespaceApi, myNamespace) {
    $scope.model = {
      name: '',
      description: ''
    };
    $scope.submitHandler = function() {
      myNamespaceApi.create({
        namespaceId: $scope.model.id
      },{
        id: $scope.model.id,
        name: $scope.model.name,
        description: $scope.model.description
      }, function(res) {
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
