/**
 * Operation 2.8
 */

angular.module(PKG.name+'.feature.operation28')
  .factory('op28helper', function () {

    function panelMap (d) {
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
    };


    function pollCb (res) {
      var v = res.data.map(function (o) {
        return {
          time: o.time,
          y: o.value
        };
      });
      if(this.history) {
        this.stream = v.slice(-1); // because poll is once per second
      }
      this.history = [{
        label: this.metric,
        values: v
      }];
      this.lastValue = res.data.pop().value;
    };

    return {
      panelMap: panelMap,
      pollCb: pollCb
    };

  })
  .controller('Op28CdapCtrl', function ($scope, op28helper, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    $scope.panels = [
      ['Collect', 'EPS',    'collect.events'],
      ['Process', '%',      'process.busyness'],
      ['Store', 'B/s',      'dataset.store.bytes'],
      ['Query', 'QPS',      'query.requests']
    ].map(op28helper.panelMap);

    angular.forEach($scope.panels, function (panel) {
      var c = panel.chart;
      dataSrc.poll({
          _cdapPathV2: '/metrics/system/'
            + c.metric + '?start=now-60s&end=now',
          method: 'GET'
        },
        op28helper.pollCb.bind(c)
      );
    });

  })
  .controller('Op28SystemCtrl', function ($scope, op28helper, MyDataSource) {

    $scope.panels = [
      ['AppFabric', 'Containers', ''],
      ['Processors', 'Cores',     ''],
      ['Memory', 'B',             ''],
      ['DataFabric', 'GB',        '']
    ].map(op28helper.panelMap);

    // angular.forEach($scope.panels, function (panel) {
    //   var c = panel.chart;
    //   dataSrc.poll({
    //       _cdapPathV2: '/metrics/system/'
    //         + c.metric + '?start=now-60s&end=now',
    //       method: 'GET'
    //     },
    //     op28helper.pollCb.bind(c)
    //   );
    // });

  })
  .controller('Op28AppsCtrl', function ($scope, MyDataSource) {


    console.log('TODO: Op28AppsCtrl');


  })

