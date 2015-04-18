// adapted from https://github.com/dainbrump/ng-epoch
var ngEpoch = angular.module(PKG.name+'.commons');

var baseDirective = {
  restrict: 'E',
  replace: true,
  template: '<div class="epoch"></div>',
  scope: {
    history: '=',
    stream: '='
  },
  controller: 'epochController'
};

ngEpoch.factory('Epoch', function ($window) {
  return $window.Epoch;
});

ngEpoch.controller('epochController', function ($scope, $compile, caskWindowManager, Epoch) {

  $scope.initEpoch = function (elem, type, attr, forcedOpts) {
    if($scope.me) {
      return;
    }

    var options = {};

    angular.forEach(attr, function (v, k) {
      if ( v && k.indexOf('chart')===0 ) {
        var key = k.substring(5);
        this[key.charAt(0).toLowerCase() + key.slice(1)] = $scope.$eval(v);
      }
    }, options);

    angular.extend(options, forcedOpts || {}, {
      data: angular.copy($scope.history),
      el: elem[0]
    });

    if(!options.data) {
      options.data = [{values:[{
        time: Math.floor(Date.now() / 1000),
        y: 0
      }]}];
    }

    var formatDate = function (date) {
      var hours = date.getHours();
      var minutes = date.getMinutes();
      var seconds = date.getSeconds();
      var ampm = hours >= 12 ? 'PM' : 'AM';
      hours = hours % 12;
      hours = hours ? hours : 12;
      minutes = minutes < 10 ? '0' + minutes : minutes;
      seconds = seconds < 10 ? '0' + seconds : seconds;
      // example format: 1:09:11 PM
      return hours + ':' + minutes + ':' + seconds + ' ' + ampm;
    }

    options.tickFormats = { bottom: function(d) { return formatDate(new Date(d * 1000)); } };

    $scope.type = type;
    $scope.options = options;

    if(attr.history) {
      var once = false;
      $scope.$watch('history', function (newVal) {
        if(newVal) {
          $scope.options.data = newVal;
          if(!once) {
            once = true;
            render();
          }
        }
      });
    }

    render();

    if(attr.stream) { // presence of attribute determines "liveness"
      $scope.$watch('stream', function (newVal) {
        if(!$scope.me) {
          return;
        }
        if (type === 'time.gauge') {
          $scope.me.update(newVal);
        }
        else if (newVal && newVal.length) {
          $scope.me.push(newVal);
        }
      });
    }
  };

  function render () {
    var o = $scope.options,
        el = angular.element(o.el).empty();
    console.log('[epoch]', $scope.type, o);
    $scope.me = new Epoch._typeMap[$scope.type](o);
    $scope.me.draw();
    $compile(el)($scope);
  }

  $scope.$on(caskWindowManager.event.resize, render);

});


ngEpoch.directive('epochPie', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initEpoch(elem, 'pie', attr);
    }
  }, baseDirective);
});



ngEpoch.directive('epochTimeBar', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initEpoch(elem, 'time.bar', attr);
    }
  }, baseDirective);
});


ngEpoch.directive('epochTimeLine', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initEpoch(elem, 'time.line', attr);
    }
  }, baseDirective);
});


ngEpoch.directive('epochLine', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initEpoch(elem, 'line', attr);
    }
  }, baseDirective);
});



ngEpoch.directive('epochTimeArea', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initEpoch(elem, 'time.area', attr);
    }
  }, baseDirective);
});


ngEpoch.directive('epochGauge', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.initEpoch(elem, 'time.gauge', attr, {
        domain: [0, 1000],
        format: function(v) { return v.toFixed(2); }
      });
    }
  }, baseDirective);
});


