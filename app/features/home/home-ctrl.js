/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $alert, MyDataSource) {

  var dataSrc = new MyDataSource($scope);

  $scope.url = '/whatever';

  $scope.fetchSomething = function (url) {
    dataSrc.fetch('something', {url:url});

  };

  dataSrc.poll('pollthing', {url:'/pollme'})

});



