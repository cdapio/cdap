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

var ngC3 = angular.module(PKG.name+'.commons');

var baseDirective = {
  restrict: 'E',
  replace: true,
  template: '<div class="c3"></div>',
  scope: {
    chartMetric: '=',
    chartMetricAlias: '@',
    chartSize: '=',
    chartSettings: '=',
    chartPadding: '=?'
  },
  controller: 'c3Controller'
};


ngC3.factory('c3', function ($window) {
  return $window.c3;
});

ngC3.controller('c3Controller', function ($scope, c3, $filter, $timeout) {
  // We need to bind because the init function is called directly from the directives below
  // and so the function arguments would not otherwise be available to the initC3 and render functions.
  /* jshint shadow:true */
  var c3 = c3,
      $filter = $filter;

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
      drawChart();
    }
    $scope.$watch('chartMetric', drawChart, true);

  };

  function drawChart() {
    if (!$scope.chartMetric) {
      return;
    }

    $scope.options.data = $scope.chartMetric;
    $timeout(function () {
      render();
    });

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
    if ($scope.chartPadding) {
      chartConfig.padding = $scope.chartPadding;
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
    chartConfig.spline = {
      interpolation: { type: 'monotone' }
    };
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
