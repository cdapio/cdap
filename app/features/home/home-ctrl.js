/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $alert, MyDataSource) {

  var dataSrc = new MyDataSource($scope);

  $scope.something = null;

  dataSrc.poll('something', {
    url: '/v2/metrics',
    method: 'POST',
    body: ["/system/apps/HelloWorld/process.events.processed?start=now-60s&end=now-0s&count=60"]
  });

});



