var ngC3 = angular.module(PKG.name+'.commons');

var baseDirective = {
  restrict: 'E',
  replace: true,
  template: '<div class="c3"></div>',
  scope: {
    history: '='
  },
  controller: 'c3Controller'
};


ngC3.factory('c3', function ($window) {
  return $window.c3;
});

ngC3.controller('c3Controller', function ($scope, caskWindowManager, c3, myHelpers, $filter) {
  // We need to bind because the init function is called directly from the directives below
  // and so the function arguments would not otherwise be available to the initC3 and render functions.
  var caskWindowManager = caskWindowManager;
  var c3 = c3;
  var myHelpers = myHelpers;
  var $filter = $filter;

  $scope.initC3 = function (elem, type, attr, forcedOpts) {
    if($scope.me) {
      return;
    }

    // Default options:
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


    if(attr.history) {
      $scope.$watch('history', function (newVal) {
        if(newVal && newVal.length) {
          // format Data as acceptable by c3.
          // TODO: if we get rid of epoch, we can more easily change the format of 'history',
          // to avoid an intermediary data format
          var columns = [];
          var metricNames = [];
          // Iterate over the different metrics in each chart
          for (var i = 0; i < newVal.length; i++) {
            var thisMetric = newVal[i];
            var thisData = [];
            thisData.push(thisMetric.label);
            metricNames.push(thisMetric.label);
            // For each metric, iterate over all the time values, to construct an array in the format:
            // [metricName, v1, v2, v3]
            for (var j = 0; j < thisMetric.values.length; j++) {
              thisData.push(thisMetric.values[j].y);
            }
            columns.push(thisData);
          }
          // Need to construct an array in the format: ['x', ts1, ts2, ts3],
          // so extract the timestamp values from the first metric's timeseries.
          var xValues = ['x'];
          for (var i = 0; i < newVal[0].values.length; i++) {
            xValues.push(newVal[0].values[i].time);
          }
          columns.push(xValues);


          myData = { x: 'x', columns: columns, keys: {x: 'x'} };

          if ($scope.options.stack) {
            myData.groups = [metricNames];
          }

          // Save the data for when it gets resized.
          $scope.options.data = myData;

          render()
          // WARN: using load() API has funny animation (when inserting new data points to the right)
//            $scope.me.load(myData);  // Alternative to render()
        }
      });
    }
    render();
  };

  function render() {
    var data = $scope.options.data;
    data.type = $scope.type;

    // Mainly needed for pie chart values to be shown upon tooltip, but also useful for other types.
    var myTooltip = {
      format: {
        value: function (value, ratio, id) {
          return d3.format(',')(value);
        }
      }
    }

    var chartConfig = {bindto: $scope.options.el, data: data, tooltip: myTooltip};
    chartConfig.size = $scope.options.size;

    var xTick = {};
    if ($scope.options.formatAsTimestamp) {
      var timestampFormat = function(timestamp) {
        return $filter('amDateFormat')(timestamp, 'h:mm:ss a');
      };
      xTick.format = timestampFormat;
    }
    chartConfig.axis = { x: { show: $scope.options.showx,
                              tick : xTick },
                         y: { show: $scope.options.showy } };
    chartConfig.color = $scope.options.color;
    chartConfig.legend = $scope.options.legend;
    chartConfig.point = { show: false };
    $scope.me = c3.generate(chartConfig);
  }

  $scope.$on(caskWindowManager.event.resize, render);

});

ngC3.directive('c3Line', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'line', attr);
    }
  }, baseDirective);
});

ngC3.directive('c3Bar', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'bar', attr, {stack: true});
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

ngC3.directive('c3Spline', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'spline', attr);
    }
  }, baseDirective);
});

ngC3.directive('c3Step', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'step', attr);
    }
  }, baseDirective);
});

ngC3.directive('c3Area', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area', attr);
    }
  }, baseDirective);
});

ngC3.directive('c3AreaStep', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area-step', attr);
    }
  }, baseDirective);
});

ngC3.directive('c3AreaSpline', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'area-spline', attr);
    }
  }, baseDirective);
});
