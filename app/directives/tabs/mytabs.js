angular.module(PKG.name + '.commons')
  .directive('myTabs', function() {
    return {
      restrict: 'EA',
      scope: {
        tabsPartialPath: '@',
        tabsList: '&'
      },
      controller: 'myTabsCtrl',
      templateUrl: 'tabs/mytabs.html'
    };
  });
