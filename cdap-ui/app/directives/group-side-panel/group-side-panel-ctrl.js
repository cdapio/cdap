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
    this.groupGenericName = $scope.groupGenericName || 'group';
    this.itemGenericName = $scope.itemGenericName || 'item';

    this.view = $scope.view || 'icon';
    $scope.$watch('MySidePanel.groups.length', function() {

      if (this.groups.length) {
        this.openedGroup = this.groups[0].name;
      }
      /*
        42 = height of the each group's header
        (-42) = height of the current wrapper(group's) header height. We need to include that in the height of the group's wrapper.
        This is has to be through ng-style as the #of groups we might have could be dynamic and having to fit all in one specific
        height needs this calculation.

        FIXME: This will not scale i.e., non-reusable.

      */
      this.groupWrapperHeight = 'calc(100% - '+ (((this.groups.length * 35) - 35) - 1)+ 'px)';
    }.bind(this));

    this.onItemClicked = function(event, item) {
      event.stopPropagation();
      event.preventDefault();
      var fn = $scope.onPanelItemClick();
      if ('undefined' !== typeof fn) {
        fn.call($scope.onPanelItemClickContext, event, item);
      }
    };
  });
