angular.module(PKG.name + '.commons')
  .controller('MySidePanel', function ($scope) {
    this.groups = $scope.panelGroups;
    this.placement = $scope.placement;
    this.panel = $scope.panel;

    this.isSubMenu = $scope.isSubMenu === 'true';
    this.isExpanded = $scope.isExpanded === 'true';

    this.openGroup = function (group) {
      if (this.openedGroup === group.name && this.showGroupItems) {
        this.showGroupItems = false;
        this.openedGroup = null;
        return;
      }
      this.openedGroup = group.name;
      var fn = $scope.onGroupClick();
      if ('undefined' !== typeof fn) {
        fn.call($scope.onGroupClickContext, group);
      }
    };

    this.onItemClicked = function(event, item) {
      var fn = $scope.onPanelItemClick();
      if ('undefined' !== typeof fn) {
        fn.call($scope.onPanelItemClickContext, event, item);
      }
    };
    if (this.isSubMenu) {
      this.openGroup(this.groups[0]);
    }

  });
