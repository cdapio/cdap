angular.module(PKG.name + '.commons')
  .directive('myTabs', function() {
    return {
      restrict: 'EA',
      scope: true,
      controller: 'myTabsCtrl',
      templateUrl: 'tabs/mytabs.html'
    };
  });
