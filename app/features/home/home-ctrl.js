/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $alert, MyDataSource, $interval) {

  var dataSrc = new MyDataSource($scope);

  $scope.lineHistory = null;
  $scope.lineStream = null;
  $scope.gaugeValue = 0;

  dataSrc.poll({
      url: '/v2/metrics',
      method: 'POST',
      body: ["/system/apps/HelloWorld/process.events.processed?start=now-61s&end=now-1s"]
    },
    function (result) {
      var d = result && result[0].result.data;
      if(d.length) {
        var v = d[d.length-1],
            convert = function (h) {
              return {
                timestamp: h.time, // d3 requires GMT ?
                y: h.value
              };
            };

        $scope.gaugeValue = v.value;

        if($scope.lineHistory) {
          $scope.lineStream = [convert(v)];
        }
        else {
          $scope.lineHistory = [{
            label: 'HelloWorld',
            values: d.map(convert)
          }];
        }

      }
    }
  );

});





