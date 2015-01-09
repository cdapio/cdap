angular.module(PKG.name + '.commons')
  .directive('myMetricSearch', function () {
    return {
      restrict: 'E',
      scope: {
        model: '='
      },
      templateUrl: 'metric-search/metric-search.html'
    };
  });
