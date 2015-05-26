/**
 * Widget model & controllers
 */

angular.module(PKG.name+'.feature.dashboard')
  .factory('Widget', function (MyDataSource, myHelpers) {

    function Widget (opts) {
      opts = opts || {};
      this.title = opts.title || 'Widget';

      // Type of widget and the metrics
      this.type = opts.type;
      this.metric = opts.metric || false;
      this.metricAlias =  opts.metricAlias || {};

      // Dimensions and Attributes of a widget
      this.settings = {};
      opts.settings = opts.settings || {}; // Not a required paramter.
      this.settings.color = opts.settings.color;

      if (myHelpers.objectQuery(opts, 'settings', 'size')) {
        this.width = opts.settings.size.width;
        this.height = opts.settings.size.height;
      } else {
        this.width = '';
        this.height = 200;
      }

      this.settings.chartMetadata = {};
      if (opts.settings.chartMetadata) {
        this.settings.chartMetadata = opts.settings.chartMetadata;
      }

      // Should the widget be live or not.
      this.settings.isLive = opts.settings.isLive || false;
      // Based on Live or not what is the interval at which to poll
      // and how should the value be aggregated
      // (if we get 60 values but want to aggregate it to show only 5)
      this.settings.interval = opts.settings.interval;
      this.settings.aggregate = opts.settings.aggregate;
    }

    Widget.prototype.getPartial = function () {
      return '/assets/features/dashboard/templates/widgets/' + this.type + '.html';
    };

    return Widget;
  })

  .controller('DropdownCtrl', function ($scope, $state, $dropdown) {
    $scope.ddWidget = function(event){
      var toggle = angular.element(event.target);
      if(!toggle.hasClass('dropdown-toggle')) {
        toggle = toggle.parent();
      }

      var scope = $scope.$new(),
          dd = $dropdown(toggle, {
            template: 'assets/features/dashboard/templates/partials/wdgt-dd.html',
            animation: 'am-flip-x',
            trigger: 'manual',
            prefixEvent: 'wdgt-tab-dd',
            scope: scope
          });

      dd.$promise.then(function(){
        dd.show();
      });

      scope.$on('wdgt-tab-dd.hide', function () {
        dd.destroy();
      });
    };
  })

  // Probably should be renamed or refactored. This is not doing anything apart
  // from handling size changes to the widget.
  .controller('C3WidgetTimeseriesCtrl', function ($scope, myHelpers, $timeout) {
    $scope.chartSize = { height: 200 };
    var widget = myHelpers.objectQuery($scope, 'gridsterItem', '$element', 0),
        widgetHeight;
    if (widget) {
      widgetHeight = parseInt(widget.style.height, 10);
      widgetWidth = parseInt(widget.style.width, 10);
      if (widgetHeight > 300) {
        $scope.wdgt.height = widgetHeight - 70;
      }
      $scope.wdgt.width = widgetWidth - 32;
    }

    $scope.$on('gridster-resized', function(event, sizes) {
      $timeout(function() {
        $scope.chartSize.height = parseInt($scope.gridsterItem.$element[0].style.height, 10) - 70;
        $scope.chartSize.width = parseInt($scope.gridsterItem.$element[0].style.width, 10) - 32;
      });
    });

    $scope.$watch('wdgt.height', function(newVal) {
      $scope.chartSize.height = newVal;
    });
    $scope.$watch('wdgt.width', function(newVal) {
      if (!newVal) {
        return;
      }
      $scope.chartSize.width = newVal;
    });
  })

  .controller('WidgetTableCtrl', function ($scope, MyDataSource, MyChartHelpers, MyMetricsQueryHelper) {

    $scope.metrics = $scope.wdgt.metric;
    var dataSrc = new MyDataSource($scope);

    dataSrc.request(
      {
        _cdapPath: '/metrics/query',
        method: 'POST',
        body: MyMetricsQueryHelper.constructQuery(
          'qid',
          MyMetricsQueryHelper.contextToTags($scope.metrics.context),
          $scope.metrics
        )
      }
    )
      .then(function(res) {
        var processedData = MyChartHelpers.processData(
          res,
          'qid',
          $scope.metrics.names,
          $scope.metrics.resolution
        );
        processedData = MyChartHelpers.c3ifyData(processedData, $scope.metrics, {});
        var tableData = [];
        processedData.xCoords.forEach(function(timestamp, index) {
          if (index === 0) {
            // the first index of each column is just 'x' or the metric name
            return;
          }
          var rowData = [timestamp];
          processedData.columns.forEach(function(column) {
            // If it begins with 'x', it is timestamps
            if (column.length && column[0] !== 'x') {
              rowData.push(column[index]);
            }
          });
          tableData.push(rowData);
        });
        $scope.tableData = tableData;
      });
  })

  .controller('WidgetPieCtrl', function ($scope, $alert) {

    $alert({
      content: 'pie chart using fake data',
      type: 'warning'
    });

    $scope.pieChartData = [
      { label: 'Slice 1', value: 10 },
      { label: 'Slice 2', value: 20 },
      { label: 'Slice 3', value: 40 },
      { label: 'Slice 4', value: 30 }
    ];

  });
