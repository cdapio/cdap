var commonModule = angular.module(PKG.name+'.commons');
commonModule.factory('jsPlumb', function ($window) {
  return $window.jsPlumb;
});

commonModule.directive('myPlumb', function() {
  return {
    restrict: 'E',
    scope: {
      config: '='
    },
    templateUrl: 'plumb/my-plumb.html',
    controller: 'MyPlumbController',
    controllerAs: 'MyPlumbController'
  };
});
