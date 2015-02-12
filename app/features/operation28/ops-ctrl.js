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
            history: null
          }
        }
      });

    angular.forEach($scope.panels, function (panel) {
      var c = panel.chart;
      dataSrc.request(
        {
          _cdapPathV2: '/metrics/system/'+c.metric+'?start=now-65s&end=now-5s',
          method: 'GET'
        },
        function (res) {
          c.history = [
            {
              label: c.metric,
              values: res.data.map(function (o) {
                return {
                  time: o.time,
                  y: o.value
                };
              })
            }
          ];
        }
      );

    });

  })
  .controller('Op28SystemCtrl', function ($scope, $state) {





  })
  .controller('Op28AppsCtrl', function ($scope, $state) {





  })

