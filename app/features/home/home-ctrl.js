/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $alert, MyDataSource) {

  var dataSrc = new MyDataSource($scope);

  $scope.url = '/whatever';

  $scope.something = {hello:'world'};

  $scope.fetchSomething = function (url) {
    dataSrc.fetch('something.result', {url:url});
  };

  dataSrc.poll('pollthing', {url:'/pollme'})

});



