/**
 * Operation 2.8
 */

angular.module(PKG.name+'.feature.dashboard')
/* ------------------------------------------------------ */
  .controller('OpsCdapCtrl',
  function ($scope, opshelper, $rootScope) {
    var panels = [
   // Format:
   // [ Widget Title, context, [metricNames], line-type (options are in addwdgt-ctrl.js ]
      [
        'Router requests',
        '',
        ['system.request.received','system.response.client-error', 'system.response.successful'], 'c3-scatter',
        { sizeX: 4, sizeY: 2, row: 0, col: 0 }
      ],
      [
        'Dataset Service',
        'component.dataset~service', ['system.request.received','system.response.client-error','system.response.successful'],  'c3-scatter',
        { sizeX: 2, sizeY: 2, row: 0, col: 4 }
      ],
      [
        'Transaction Commit',
        '',
        ['system.canCommit', 'system.commit', 'system.start.long', 'system.start.short'], 'c3-area-spline',
        { sizeX: 2, sizeY: 1, row: 2, col: 0 }
      ],
      [
        'Transaction Latency',
        '',
        ['system.commit.latency', 'system.start.short.latency'],
        'c3-area-spline',
        { sizeX: 2, sizeY: 1, row: 2, col: 2 }
      ],
      [
        'System Error and Warnings',
        '',
        ['system.services.log.error', 'system.services.log.warn'],
        'c3-area-step',
        { sizeX: 2, sizeY: 1, row: 2, col: 4 }
      ],
      [
        'Explore Service',
        'component.explore~service',
        ['system.request.received','system.response.successful'],
        'c3-area-spline',
        { sizeX: 2, sizeY: 1, row: 3, col: 0 }
      ],
      [
        'Events Processed',
        'namespace.*',
        ['system.process.events.processed'],
        'c3-line',
        { sizeX: 4, sizeY: 1, row: 3, col: 2 }
      ],
      [
        'Bytes Store',
        'namespace.*',
        ['system.dataset.store.bytes'],
        'c3-line',
        { sizeX: 2, sizeY: 1, row: 4, col: 0 }
      ],
      [
        'Dataset Read/Writes',
        'namespace.*',
        ['system.dataset.store.writes' ,'system.dataset.store.reads'],
        'c3-area-spline',
        { sizeX: 2, sizeY: 1, row: 4, col: 2 }
      ],
      [
        'Containers Used',
        'namespace.*',
        ['system.resources.used.containers', 'system.process.instance'],
        'c3-area-step',
        { sizeX: 2, sizeY: 1, row: 4, col: 4 }
      ]
    ];

    $scope.currentBoard = opshelper.createBoardFromPanels(panels);
    $scope.gridsterOpts = {
      rowHeight: '280',
      columns: 6,
      minSizeX: 2,
      mobileBreakPoint: 800,
      swapping: true,
      resizable: {
        enabled: true,
        start: function(event, uiWidget, $element) {}, // optional callback fired when resize is started,
        resize: function(event, uiWidget, $element) {}, // optional callback fired when item is resized,
        stop: function(event, uiWidget, $element) {
          var resizedHeight = parseInt(uiWidget[0].style.height, 10),
              resizedWidth = parseInt(uiWidget[0].style.width, 10);

          $element.height = (resizedHeight < 300 ? 200: resizedHeight - 70);
          $element.width = (resizedWidth < 450? 370: resizedWidth - 32);
        } // optional callback fired when item is finished resizing
     }
    };

  })

  .controller('OpsAppsCtrl',
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
                '?tag=namespace:system' +
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

  .factory('opshelper', function (Widget, $timeout) {
    function createWidget(title, context, metricNames, type) {
      return new Widget({
        title: title,
        type: type,
        settings: {
          isLive: true,
          interval: 15000
        },
        metric: {
          context: context,
          names: metricNames,
          startTime: 'now-3600s',
          endTime: 'now',
          resolution: '1m'
        }
      });
    }

    function createBoardFromPanels(panels) {
      var widgets = [];
      panels.forEach(function(panel) {
        var widget = createWidget(panel[0], panel[1], panel[2], panel[3]);
        angular.extend(widget, panel[4]);
        widgets.push(widget);
      });
      // Note: title is not currently used in the view
      return {title : "System metrics", columns : widgets};
    }

    return {
      createBoardFromPanels: createBoardFromPanels
    };
  })

  ;
