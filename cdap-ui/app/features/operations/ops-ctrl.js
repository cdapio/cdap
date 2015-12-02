/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Operation 2.8
 */

angular.module(PKG.name+'.feature.dashboard')
/* ------------------------------------------------------ */
  .controller('OpsCdapCtrl',
  function ($scope, opshelper) {
    var panels = [
   // Format:
   // [ Widget Title, context, [metricNames], line-type (options are in addwdgt-ctrl.js ]
      [
        'Router Requests',
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
        'Transaction Commits',
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
        'System Errors and Warnings',
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
        'Bytes Stored',
        'namespace.*',
        ['system.dataset.store.bytes'],
        'c3-line',
        { sizeX: 2, sizeY: 1, row: 4, col: 0 }
      ],
      [
        'Dataset Reads and Writes',
        'namespace.*',
        ['system.dataset.store.reads' ,'system.dataset.store.writes'],
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
        stop: function(event, uiWidget, $element) {
          var resizedHeight = parseInt(uiWidget[0].style.height, 10),
              resizedWidth = parseInt(uiWidget[0].style.width, 10);

          $element.height = (resizedHeight < 300 ? 200: resizedHeight - 70);
          $element.width = (resizedWidth < 450? 370: resizedWidth - 32);
        } // optional callback fired when item is finished resizing
      }
    };

    angular.forEach($scope.currentBoard.columns, function (widget) {
      opshelper.startPolling(widget);
    });

    $scope.$on('$destroy', function() {
      angular.forEach($scope.currentBoard.columns, function (widget) {
        opshelper.stopPolling(widget);
      });
    });

  })

  .controller('OpsAppsCtrl',
  function ($scope, $state, myHelpers, MyCDAPDataSource) {

    var dataSrc = new MyCDAPDataSource($scope);

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

  .factory('opshelper', function (Widget, MyCDAPDataSource, MyMetricsQueryHelper, MyChartHelpers) {
    var dataSrc = new MyCDAPDataSource();

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
      return {title : 'System Metrics', columns : widgets};
    }

    function startPolling (widget) {
      widget.pollId = dataSrc.poll({
        _cdapPath: '/metrics/query',
        method: 'POST',
        body: MyMetricsQueryHelper.constructQuery(
          'qid',
          MyMetricsQueryHelper.contextToTags(widget.metric.context),
          widget.metric
        )
      }, function (res) {
        var processedData = MyChartHelpers.processData(
          res,
          'qid',
          widget.metric.names,
          widget.metric.resolution,
          widget.settings.aggregate
        );

        processedData = MyChartHelpers.c3ifyData(processedData, widget.metric, widget.metricAlias);
        var data = {
          x: 'x',
          columns: processedData.columns,
          keys: {
            x: 'x'
          }
        };
        widget.formattedData = data;

      }).__pollId__;
    }

    function stopPolling (widget) {
      dataSrc.stopPoll(widget.pollId);
    }

    return {
      createBoardFromPanels: createBoardFromPanels,
      startPolling: startPolling,
      stopPolling: stopPolling
    };
  })

  ;
