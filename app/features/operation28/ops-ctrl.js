/**
 * Controllers for Operation 2.8
 */


angular.module(PKG.name+'.feature.operation28')
  .controller('Op28CdapCtrl', function ($scope, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    $scope.panels = [
      ['Collect', 'EPS', 'collect.events'],
      ['Process', '%', 'process.busyness'],
      ['Store', 'B/s', 'dataset.store.bytes'],
      ['Query', 'QPS', 'query.requests'],
    ].map(function(d){
        return {
          title: d[0],
          unit: d[1],
          chart: {
            metric: d[2],
            context: 'system',
            history: null,
            stream: null,
            lastValue: 0
          }
        }
      });

    angular.forEach($scope.panels, function (panel) {
      var c = panel.chart,
          path = '/metrics/system/' + c.metric,
          firstTime = true;

      dataSrc.poll({
          _cdapPathV2: path + '?start=now-61s&end=now-1s',
          method: 'GET'
        },
        function (res) {
          var v = res.data.map(function (o) {
            return {
              time: o.time,
              y: o.value
            };
          });
          if(firstTime) {
            c.history = [{
              label: c.metric,
              values: v
            }];
            firstTime = false;
          }
          else {
            c.stream = v.slice(-10);
          }
          c.lastValue = res.data.pop().y;
        }
      );

    });

  })
  .controller('Op28SystemCtrl', function ($scope, $state) {





  })
  .controller('Op28AppsCtrl', function ($scope, $state) {





  })

