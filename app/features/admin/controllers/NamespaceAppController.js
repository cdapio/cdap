angular.module(PKG.name + '.feature.admin').controller('AdminNamespaceAppController',
function ($scope, $state, myAppUploader, MyDataSource, myNamespace) {

  $scope.apps = [];
  $scope.nsname = myNamespace.getDisplayName($state.params.nsadmin);
  var myDataSrc = new MyDataSource($scope);

  myDataSrc.request({
    _cdapPath: '/namespaces/' + $state.params.nsadmin + '/apps'
  })
    .then(function(response) {
      $scope.apps = response;

    });

  $scope.onFileSelected = function(files) {
    myAppUploader.upload(files, $state.params.nsadmin);
  };
});
