var ngC3 = angular.module(PKG.name+'.commons');

var baseDirective = {
  restrict: 'E',
  replace: true,
  template: '<div class="c3"></div>',
  scope: {
    chartMetric: '=',
    chartMetricAlias: '@',
    chartSize: '='
  },
  controller: 'c3Controller'
};


ngC3.factory('c3', function ($window) {
  return $window.c3;
});

ngC3.controller('c3Controller', function ($scope, c3, myHelpers, $filter, $timeout, MyChartHelpers, MyDataSource) {
  // We need to bind because the init function is called directly from the directives below
  // and so the function arguments would not otherwise be available to the initC3 and render functions.
  var c3 = c3,
      myHelpers = myHelpers,
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

  $scope.initC3 = function (elem, type, attr, forcedOpts) {

    // Default options:
    // The queryId value does not matter, as long as we are using the same value in the request
    // as in parsing the response.
    var options = {stack: false, formatAsTimestamp: true};
    angular.extend(options, forcedOpts || {}, {
      el: elem[0]
    });

    angular.forEach(attr, function (v, k) {
     if ( v && k.indexOf('chart')===0 ) {
       var key = k.substring(5);
       this[key.charAt(0).toLowerCase() + key.slice(1)] = $scope.$eval(v);
     }
   }, options);

    options.data = { x: 'x', columns: [] };

    $scope.type = type;
    $scope.options = options;

    $scope.$watch('chartSize', function(newVal) {
      $scope.options.size = newVal;
      $timeout(function() {
        render();
      });
    }, true);

    if ($scope.metrics) {
      $scope.fetchData()
        .then(function(res) {
          var myData;
          var processedData = MyChartHelpers.processData(
            res,
            queryId,
            $scope.metrics.names,
            $scope.metrics.resolution,
            $scope.metrics.aggregate
          );
          processedData = MyChartHelpers.c3ifyData(processedData, $scope.metrics, $scope.alias);
          myData = { x: 'x', columns: processedData.columns, keys: {x: 'x'} };

          if ($scope.options.stack) {
            myData.groups = [processedData.metricNames];
          }

          // Save the data for when it gets resized.
          $scope.options.data = myData;
          $scope.clearChart();
          $timeout(function() {
            render();
          });
        });
    }
  };

  $scope.fetchData = function () {
    return dataSrc.request({
      _cdapPath: '/metrics/query',
      method: 'POST',
      body: constructQuery(queryId, contextToTags($scope.metrics.context), $scope.metrics)
    });
  };

  // 'ns.default.app.foo' -> {'ns': 'default', 'app': 'foo'}
  function contextToTags(context) {
    var parts, tags, i, tagValue;
    if (context.length) {
      parts = context.split('.');
    } else {
      // For an empty context, we want no tags. Splitting it by '.' yields [""]
      parts = [];
    }
    if (parts.length % 2 !== 0) {
      throw "Metrics context has uneven number of parts: " + context;
    }
    tags = {};
    for (i = 0; i < parts.length; i+=2) {
      // In context, '~' is used to represent '.'
      tagValue = parts[i + 1].replace(/~/g, '.');
      tags[parts[i]] = tagValue;
    }
    return tags;
  }

  function constructQuery(queryId, tags, metric) {
    var timeRange, retObj;
    timeRange = {'start': metric.startTime || 'now-60s',
                'end': metric.endTime || 'now'};
    if (metric.resolution) {
      timeRange.resolution = metric.resolution;
    }
    retObj = {};
    retObj[queryId] = {tags: tags, metrics: metric.names, groupBy: [], timeRange: timeRange};
    return retObj;
  }


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
                         y: { show: $scope.options.showy } };
    chartConfig.color = $scope.options.color;
    chartConfig.legend = $scope.options.legend;
    chartConfig.point = { show: false };
    if($scope.options.subchart) {
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
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3Bar', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      // xtickcount option does not work for bar charts, so have to use culling
      scope.initC3(elem, 'bar', attr, {stack: true, xTickCulling: {max: 5}});
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3Pie', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'pie', attr, {formatAsTimestamp: false});
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3Donut', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'donut', attr, {formatAsTimestamp: false});
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3Scatter', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'scatter', attr, {xtickcount: 5});
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3Spline', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'spline', attr, { xtickcount: 5});
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3Step', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'step', attr, {xtickcount: 5});
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3Area', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area', attr, {xtickcount: 5});
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3AreaStep', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area-step', attr, {xtickcount: 5});
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3AreaSpline', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area-spline', attr, {xtickcount: 5} );
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});

ngC3.directive('c3AreaSplineStacked', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area-spline', attr, {stack: true, xtickcount: 5});
      scope.clearChart = function() {
        while(elem.firstChild) {
          elem.removeChild(elem.firstChild);
        }
      }
    }
  }, baseDirective);
});
