/**
 * Operation 2.8
 */

angular.module(PKG.name+'.feature.operation28')
/* ------------------------------------------------------ */

  .controller('Op28CdapCtrl',
  function ($scope, $state, op28helper, MyDataSource) {

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
          _cdapPathV2: '/metrics/system/' +
              c.metric + '?start=now-60s&end=now',
          method: 'GET'
        },
        op28helper.pollCb.bind(c)
      );
    });

  })


/* ------------------------------------------------------ */


  .controller('Op28SystemCtrl',
  function ($scope, op28helper, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    $scope.panels = [
      ['AppFabric', 'Containers', 'containers'],
      ['Processors', 'Cores',     'vcores'],
      ['Memory', 'B',             'memory'],
      ['DataFabric', 'B',         'storage']
    ].map(op28helper.panelMap);

    angular.forEach($scope.panels, function (panel) {
      var c = panel.chart;
      dataSrc.poll({
          _cdapPathV2: '/metrics/system/resources.used.'
            + c.metric + '?start=now-60s&end=now',
          method: 'GET'
        },
        op28helper.pollCb.bind(c)
      );
    });

  })


/* ------------------------------------------------------ */


  .controller('Op28AppsCtrl',
  function ($scope, $state, $q, myHelpers, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    $scope.apps = [];

    dataSrc.request({
        _cdapNsPath: '/apps'
      })
      .then(function (apps) {
        $scope.apps = apps;
        $scope.metrics = {};

        var p = [], // array of promises
            m = ['vcores', 'containers', 'memory'];

        for (var i = 0; i < m.length; i++) {

          p.push(
            dataSrc
              .request({
                _cdapPath: '/metrics/query' +
                  '?context=namespace.system' +
                  '&metric=system.resources.used.' +
                  m[i] + '&groupBy=app',
                method: 'POST'
              },
              function (r) {
                angular.forEach(r.series, function (s) {
                  myHelpers.deepSet(
                    $scope.apps.filter(function (one) {
                      return one.id === s.grouping.app;
                    })[0],
                    'metric.' + s.metricName.split('.').pop(),
                    s.data[0].value
                  );
                });
              })
          );
        };

        return $q.all(p);
      });


  })


/* ------------------------------------------------------ */


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
      };
    }


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
    }

    return {
      panelMap: panelMap,
      pollCb: pollCb
    };

  })

  ;
