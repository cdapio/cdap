angular.module(PKG.name + '.commons')
  .directive('myTab', function() {
    return {
      restrict: 'EA',
      scope: true,
      controller: 'myTabCtrl',
      templateUrl: 'tabs/mytabs.html'
    };
  });
