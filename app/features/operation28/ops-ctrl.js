/**
 * Operation 2.8
 */

angular.module(PKG.name+'.feature.operation28')
/* ------------------------------------------------------ */

  .controller('Op28CdapCtrl',
  function ($scope, $state, op28helper, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    $scope.panels = [
      ['Collect', 'EPS',   'collect.events'],
      ['Process', '%',     'process.busyness'],
      ['Store',   '/S',    'dataset.store.bytes', true],
      ['Query',   'QPS',   'query.requests']
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
      ['AppFabric',  'Containers', 'containers'],
      ['Processors', 'Cores',      'vcores'],
      ['Memory',     '',           'memory', true],
      ['DataFabric', '',           'storage', true]
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
  function ($scope, $state, myHelpers, MyDataSource) {

    var dataSrc = new MyDataSource($scope);

    $scope.apps = [];

    dataSrc
      .request({
        _cdapNsPath: '/apps'
      },
      function (apps) {
        $scope.apps = apps;

        var m = ['vcores', 'containers', 'memory'];

        for (var i = 0; i < m.length; i++) {

          dataSrc
            .poll({
              _cdapPath: '/metrics/query' +
                '?context=namespace.system' +
                '&metric=system.resources.used.' +
                m[i] + '&groupBy=app',
              method: 'POST'
            },
            function (r) {

              angular.forEach($scope.apps, function (app) {
                angular.forEach(r.series, function (s) {
                  if(app.id === s.grouping.app) {
                    myHelpers.deepSet(
                      app,
                      'metric.' + s.metricName.split('.').pop(),
                      s.data[0].value
                    );
                  }
                });
              });

            });
        }

      });

  })


/* ------------------------------------------------------ */


  .factory('op28helper', function () {

    function panelMap (d) {
      return {
        title: d[0],
        unit: d[1],
        useByteFilter: d[3],
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
