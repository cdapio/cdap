angular.module(PKG.name + '.feature.overview')
  .controller('AppsSectionCtrl', function($scope, myAppUploader) {
    $scope.onFileSelected = myAppUploader.upload;
  });
