angular.module(PKG.name + '.feature.overview')
  .controller('EmptySectionCtrl', function($scope, myAppUploader) {
    $scope.onFileSelected = myAppUploader.upload;
  });
