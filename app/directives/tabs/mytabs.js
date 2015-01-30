angular.module(PKG.name + '.commons')
  .directive('myTabs', function() {
    return {
      restrict: 'EA',
      scope: {
        tabsPartialPath: '@',
        tabs: '&'
      },
      controller: 'myTabsCtrl',
      templateUrl: 'tabs/mytabs.html'
    };
  });
