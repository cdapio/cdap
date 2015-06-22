var ngC3 = angular.module(PKG.name+'.commons');

var baseDirective = {
  restrict: 'E',
  replace: true,
  template: '<div class="c3"></div>',
  scope: {
    chartMetric: '=',
    chartMetricAlias: '@',
    chartSize: '=',
    chartSettings: '='
  },
  controller: 'c3Controller'
};


ngC3.factory('c3', function ($window) {
  return $window.c3;
});

ngC3.controller('c3Controller', function ($scope, c3, $filter, $timeout, MyChartHelpers, MyMetricsQueryHelper, MyDataSource) {
  // We need to bind because the init function is called directly from the directives below
  // and so the function arguments would not otherwise be available to the initC3 and render functions.
  var c3 = c3,
      $filter = $filter,
      queryId = 'qid',
      dataSrc = new MyDataSource($scope);

  $scope.metrics = $scope.chartMetric;
  $scope.alias = $scope.chartMetricAlias;

  $scope.$on('$destroy', function() {
    if ($scope.chart) {
      $scope.chart = $scope.chart.destroy();
    }
  });
  $scope.pollId = null;

  $scope.initC3 = function (elem, type, attr, forcedOpts) {

    // Default options:
    // The queryId value does not matter, as long as we are using the same value in the request
    // as in parsing the response.
    var options = {stack: false, formatAsTimestamp: true};
    angular.extend(options, forcedOpts || {}, {
      el: elem[0]
    });

    options.data = { x: 'x', columns: [] };

    $scope.type = type;
    $scope.options = options;
    $scope.options.showx = $scope.chartSettings.chartMetadata.showx;
    $scope.options.showy = $scope.chartSettings.chartMetadata.showy;
    $scope.options.legend = $scope.chartSettings.chartMetadata.legend;
    $scope.options.color = $scope.chartSettings.color;

    $scope.$watch('chartSize', function(newVal) {
      $scope.options.size = newVal;
      $timeout(function() {
        render();
      });
    }, true);

    if ($scope.metrics) {
      $scope.togglePolling();
    }

    $scope.$watch('chartSettings', $scope.reconfigure, true);
    $scope.$watch('chartMetric', $scope.reconfigure, true);
  };

  $scope.drawChart = function (res) {
    var myData;
    var processedData = MyChartHelpers.processData(
      res,
      queryId,
      $scope.metrics.names,
      $scope.metrics.resolution,
      $scope.chartSettings.aggregate
    );
    processedData = MyChartHelpers.c3ifyData(processedData, $scope.metrics, $scope.alias);
    myData = { x: 'x', columns: processedData.columns, keys: {x: 'x'} };

    if ($scope.options.stack) {
      myData.groups = [processedData.metricNames];
    }

    // Save the data for when it gets resized.
    $scope.options.data = myData;
    $timeout(function() {
      render();
    });
  };

  $scope.reconfigure = function (newVal, oldVal) {
    if (newVal === oldVal) {
      return;
    }
    $scope.togglePolling();
  };

  $scope.togglePolling = function() {
    $scope.stopPolling();
    if ($scope.chartSettings.isLive) {
      $scope.startPolling();
    } else {
      $scope.fetchData()
            .then($scope.drawChart);
    }
  };

  $scope.stopPolling = function() {
    if ($scope.pollId) {
      dataSrc.stopPoll($scope.pollId);
      $scope.pollId = null;
    }
  };

  $scope.startPolling = function() {
    var promise = dataSrc.poll({
      _cdapPath: '/metrics/query',
      method: 'POST',
      body: MyMetricsQueryHelper.constructQuery(
        queryId,
        MyMetricsQueryHelper.contextToTags($scope.metrics.context),
        $scope.metrics
      ),
      interval: $scope.chartSettings.interval
    }, $scope.drawChart);
    $scope.pollId = promise.__pollId__;
  };

  $scope.fetchData = function () {
    return dataSrc.request({
      _cdapPath: '/metrics/query',
      method: 'POST',
      body: MyMetricsQueryHelper.constructQuery(
        queryId,
        MyMetricsQueryHelper.contextToTags($scope.metrics.context),
        $scope.metrics
      )
    });
  };

  function render() {
    var data = $scope.options.data,
        myTooltip,
        chartConfig,
        timestampFormat,
        xTick = {};

    data.type = $scope.type;

    // Mainly needed for pie chart values to be shown upon tooltip, but also useful for other types.
    myTooltip = { format: { value: d3.format(',') } };

    chartConfig = {bindto: $scope.options.el, data: data, tooltip: myTooltip};
    chartConfig.size = $scope.options.size;

    xTick.count = $scope.options.xtickcount;
    xTick.culling = $scope.options.xTickCulling;
    if ($scope.options.formatAsTimestamp) {
      timestampFormat = function(timestampSeconds) {
        return $filter('amDateFormat')(timestampSeconds * 1000, 'h:mm:ss a');
      };
      xTick.format = timestampFormat;
    }
    chartConfig.axis = { x: { show: $scope.options.showx,
                              tick : xTick },
                         y: { show: $scope.options.showy,
                              padding: { bottom: 0 } } };
    chartConfig.color = $scope.options.color;
    chartConfig.legend = $scope.options.legend;
    chartConfig.point = { show: false };
    if ($scope.options.subchart) {
      chartConfig.subchart = $scope.options.subchart;
    }
    chartConfig.zoom = { enabled: false };
    chartConfig.transition = { duration: 1000 };
    chartConfig.donut = { width: 45 };
    $scope.chart = c3.generate(chartConfig);
  }

});

ngC3.directive('c3Line', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'line', attr, {xtickcount: 5});
    }
  }, baseDirective);
});

ngC3.directive('c3Bar', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      // xtickcount option does not work for bar charts, so have to use culling
      scope.initC3(elem, 'bar', attr, {stack: true, xTickCulling: {max: 5}});
    }
  }, baseDirective);
});

ngC3.directive('c3Pie', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'pie', attr, {formatAsTimestamp: false});
    }
  }, baseDirective);
});

ngC3.directive('c3Donut', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'donut', attr, {formatAsTimestamp: false});
    }
  }, baseDirective);
});

ngC3.directive('c3Scatter', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'scatter', attr, {xtickcount: 5});
    }
  }, baseDirective);
});

ngC3.directive('c3Spline', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'spline', attr, { xtickcount: 5});
    }
  }, baseDirective);
});

ngC3.directive('c3Step', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'step', attr, {xtickcount: 5});
    }
  }, baseDirective);
});

ngC3.directive('c3Area', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area', attr, {xtickcount: 5});
    }
  }, baseDirective);
});

ngC3.directive('c3AreaStep', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area-step', attr, {xtickcount: 5});
    }
  }, baseDirective);
});

ngC3.directive('c3AreaSpline', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area-spline', attr, {xtickcount: 5} );
    }
  }, baseDirective);
});

ngC3.directive('c3AreaSplineStacked', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area-spline', attr, {stack: true, xtickcount: 5});
    }
  }, baseDirective);
});
