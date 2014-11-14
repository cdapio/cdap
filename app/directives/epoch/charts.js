// adapted from https://github.com/dainbrump/ng-epoch
var ngEpoch = angular.module(PKG.name+'.commons');

var baseDirective = {
  restrict: 'E',
  replace: true,
  template: '<div class="epoch"></div>',
  scope: {
    data: '=chartData'
  },
  controller: 'epochController'
};

ngEpoch.controller('epochController', function ($scope, $compile) {

  $scope.renderEpoch = function (elem, type, attr) {
    if($scope.me) {
      return;
    }

    var options = {};

    angular.forEach(attr, function (v, k) {
      if ( v && k.indexOf('chart')===0 ) {
        var key = k.substring(5);
        this[key.charAt(0).toLowerCase() + key.slice(1)] = v;
      }
    }, options);

    angular.extend(options, {
      data: angular.copy($scope.data),
      el: elem[0]
    });

    $scope.me = new Epoch._typeMap[type](options);
    $scope.me.draw();

    $compile(elem)($scope);

    if(type.indexOf('time.')===0) {
      $scope.$watch('data', function (newVal) {
        if (newVal) {
          if ($scope.me.update) { // time.gauge
            $scope.me.update($scope.data);
          }
          else {
            $scope.me.push($scope.data);
          }
        }
      });
    }
  };

});


ngEpoch.directive('epochPie', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.renderEpoch(elem, 'pie', attr);
    }
  }, baseDirective);
});

ngEpoch.directive('epochLine', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.renderEpoch(elem, 'line', attr);
    }
  }, baseDirective);
});

ngEpoch.directive('epochLiveLine', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.renderEpoch(elem, 'time.line', attr);
    }
  }, baseDirective);
});

ngEpoch.directive('epochArea', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.renderEpoch(elem, 'area', attr);
    }
  }, baseDirective);
});

ngEpoch.directive('epochLiveArea', function () {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.renderEpoch(elem, 'time.area', attr);
    }
  }, baseDirective);
});


