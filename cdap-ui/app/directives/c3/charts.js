var ngC3 = angular.module(PKG.name+'.commons');

// I don't know what I'm doing here:
var baseDirective = {
  restrict: 'E',
  replace: true,
  template: '<div class="c3"></div>',
  scope: {
    history: '=',
    stream: '='
  },
  controller: 'c3Controller'
};


ngC3.factory('c3', function ($window) {
  return $window.c3;
});

ngC3.controller('c3Controller', function ($scope, $compile, caskWindowManager, c3) {

  $scope.initC3 = function (elem, type, attr, forcedOpts) {
    if($scope.me) {
      return;
    }

    // TODO: Implement support for chart-* options to customize chart!

    var options = {};
    angular.extend(options, forcedOpts || {}, {
      el: elem[0]
    });

    options.data = { x: 'x', columns: [] };

    $scope.type = type;
    $scope.options = options;

    // TODO: investigate the attrs of directive (used in views)
    if(attr.history) {
      var once = false;
      $scope.$watch('history', function (newVal) {
        if(newVal) {
          // format Data as acceptable by c3.
          // TODO: if we get rid of epoch, we can more easily change the format of 'history', to avoid an intermediary
          // data format
          var columns = [];
          var metricNames = [];
          for (var i = 0; i < newVal.length; i++) {
            var thisMetric = newVal[i];
            var thisData = [];
            thisData.push(thisMetric.label);
            metricNames.push(thisMetric.label);
            for (var j = 0; j < thisMetric.values.length; j++) {
              thisData.push(thisMetric.values[j].y);
            }
            columns.push(thisData);
          }
          var xValues = ['x'];
          for (var i = 0; i < newVal[0].values.length; i++) {
            xValues.push(newVal[0].values[i].time);
          }
          columns.push(xValues);


          myData = { x: 'x', columns: columns, duration: 1500, keys: {x: 'x'} };
          // for when it gets resized.
          $scope.options.data = myData;
          if(!once) {
//            $scope.options.data = myData;
//            render()

            // needed by bar-chart - NOTE: remove the array annotation to make it unstacked.
            // TODO: remove this for non-bar types (it stacks them)
            $scope.me.groups([metricNames]);

            // WARN: using load() API has funny animation (when inserting new data points to the right)
            var sTime = Date.now();
            $scope.me.load(myData);
            console.log(Date.now() - sTime);

//            uncomment following line to do flow()
//            once = true;
          } else {
            for (var i = 0; i < myData.columns.length; i++) {
              myData.columns[i] = [myData.columns[i].slice(0,1)[0], myData.columns[i].slice(-1)[0]];
            }
            // WARN: Flow API is good for inserting new data points to the right, but tooltip is not supported for flow.
            // https://github.com/masayuki0812/c3/issues/991
            $scope.me.flow(myData);
          }
        }
      });
    }

    render();
  };

  function render() {
    var data = $scope.options.data;
    data.type = $scope.type;


    // Mainly needed for pie chart values to be shown upon tooltip.
    myTooltip = {
      format: {
        value: function (value, ratio, id) {
          return d3.format(',')(value);
        }
//            value: d3.format(',') // apply this format to both y and y2
      }
    }

    var chartConfig = {bindto: $scope.options.el, data: data, tooltip: myTooltip};
    chartConfig.size = { height: 200 };
    chartConfig.axis = { x: { show: false } };
    $scope.me = c3.generate(chartConfig);
    // TODO: why were the following two lines necessary with Epoch?
//    var el = angular.element(o.el).empty();
//    $compile(el)($scope);
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
      scope.initC3(elem, 'bar', attr);
    }
  }, baseDirective);
});

ngC3.directive('c3Pie', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'pie', attr);
    }
  }, baseDirective);
});

ngC3.directive('c3Donut', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initC3(elem, 'donut', attr);
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
