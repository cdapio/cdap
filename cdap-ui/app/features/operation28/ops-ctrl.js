/**
 * Operation 2.8
 */

angular.module(PKG.name+'.feature.dashboard')
/* ------------------------------------------------------ */

  .controller('Op28CdapCtrl',
  function ($scope, op28helper, Widget) {
    var panels = [
   // Format:
   // [ Widget Title, context, [metricNames], line-type (options are in addwdgt-ctrl.js ]
      ['Collect', '', ['system.collect.events'],           'c3-line'],
      ['Process', '', ['system.process.events.processed'], 'c3-line'],
      ['Store',   '', ['system.dataset.store.bytes'],      'c3-line'],
      ['Query',   '', ['system.requests.count'],           'c3-line']
    ];

    $scope.currentBoard = op28helper.createBoardFromPanels(panels);
  })

/* ------------------------------------------------------ */

  .controller('Op28SystemCtrl',
  function ($scope, op28helper) {
    // Same format as above
    var panels = [
      ['AppFabric - Containers', '', ['system.resources.used.containers'], 'c3-line'],
      ['Processors - Cores',     '', ['system.resources.used.vcores'],     'c3-line'],
      ['Memory',                 '', ['system.resources.used.memory'],     'c3-line'],
      ['DataFabric',             '', ['system.resources.used.storage'],    'c3-line']
    ];

    $scope.currentBoard = op28helper.createBoardFromPanels(panels);
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
            }, setMetric);
        }

      });

    function setMetric(r) {

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

    }

  })

/* ------------------------------------------------------ */

  .factory('op28helper', function (Widget) {
    function createWidget(title, context, metricNames, type) {
      return new Widget({title: title, type: type, isLive: true,
        metric: {
          context: context,
          names: metricNames,
          startTime: 'now-3600s',
          endTime: 'now',
          resolution: '1m'
        },
        interval: 15000
      })
    }

    function createBoardFromPanels(panels) {
      var widgets = [];
      panels.forEach(function(panel) {
        widgets.push(createWidget(panel[0], panel[1], panel[2], panel[3]));
      });
      // Note: It doesn't seem like this matters (as long as its high enough)
      var widgetsPerRow = 2;
      var columns = [];
      for (var i = 0; i < widgetsPerRow; i++) {
        columns.push([]);
      }
      for (var i = 0; i < widgets.length; i++) {
        columns[i % widgetsPerRow].push(widgets[i]);
      }
      // Note: title is not currently used in the view
      return {title : "System metrics", columns : columns};
    }

    return {
      createBoardFromPanels: createBoardFromPanels
    };
  })

  ;
