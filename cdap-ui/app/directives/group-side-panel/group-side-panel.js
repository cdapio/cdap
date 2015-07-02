angular.module(PKG.name + '.commons')
  .directive('mySidePanel', function () {
    return {
      restrict: 'E',
      scope: {
        panelGroups: '=',
        panelConfig: '='
      },
      templateUrl: 'group-side-panel/group-side-panel.html',
      controller: 'MySidePanel',
      controllerAs: 'MySidePanel'
    };
  });
