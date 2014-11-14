/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $alert, MyDataSource, $interval) {

  var dataSrc = new MyDataSource($scope);

  $scope.something = null;
  $scope.lineData = [{values:[]}];

  dataSrc.poll('something', {
    url: '/v2/metrics',
    method: 'POST',
    body: ["/system/apps/HelloWorld/process.events.processed?start=now-60s&end=now-5s"]
  });

  // $scope.$watch('something', function (something) {
  //   if(something && something[0].result.data) {
  //     $scope.lineData = [{
  //       label: 'HelloWorld',
  //       values: something[0].result.data.map(function(o){
  //         return { time: o.time, y: o.value };
  //       })
  //     }];
  //   }
  // });

  $interval(function() {
    var now = Date.now(),
        values = [];

    for (var i = 0; i < 100; i++) {
      values.push({
        time: now+i,
        y: Math.floor(Math.random()*1000)
      });
    };

    $scope.lineData = [{
      label: 'HelloWorld',
      values: values
    }];

  }, 100);

});



