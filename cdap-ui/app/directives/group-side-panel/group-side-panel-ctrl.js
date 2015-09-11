/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.commons')
  .controller('MySidePanel', function ($scope) {
    this.groups = $scope.panelGroups;
    this.placement = $scope.placement;
    this.panel = $scope.panel;
    this.isSubMenu = $scope.isSubMenu === 'true';

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

    this.toggleSidebar = function() {
      $scope.isExpanded = !$scope.isExpanded;
    };

  });
