angular.module(PKG.name + '.feature.admin').controller('AdminNamespaceAppDetailController',
function ($scope, $state) {

  $scope.details = {
    //name: $state.params.appId,
    displayName: 'displayName',
    type: 'standard',
    version: '2.2.1',
    id: 'reallylongstringanduneditable',
    description: 'description',
    properties: [
    {
        key: 'abc',
        value: 'xyz'
    },
    {
        key: 'def',
        value: 'uvw'
    }
    ]
  };

});
