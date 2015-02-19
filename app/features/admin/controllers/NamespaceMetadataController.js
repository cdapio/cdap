angular.module(PKG.name + '.feature.admin').controller('AdminNamespaceMetadataController',
function ($scope, $state, $alert, MyDataSource) {

  var data = new MyDataSource($scope);
  var path = '/namespaces/' + $state.params.nsadmin;

  data.request({
    _cdapPath: path
  })
    .then(function (metadata) {
      $scope.metadata = metadata;
    });

  $scope.doSave = function () {
    $alert({
      title: 'it doesn\'t work yet!',
      content: 'there is no content yet',
      type: 'warning'
    });
  };

});
