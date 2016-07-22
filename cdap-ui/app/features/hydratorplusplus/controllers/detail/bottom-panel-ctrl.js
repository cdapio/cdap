/*
 * Copyright © 2016 Cask Data, Inc.
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

angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusDetailBottomPanelCtrl', function(HydratorPlusPlusBottomPanelStore, $scope, HydratorPlusPlusBottomPanelActions) {
    this.tabs = [
      {
        title: 'Log',
        template: '/assets/features/hydratorplusplus/templates/detail/tabs/log.html'
      }
    ];

    this.setIsCollapsed = function() {
      this.bottomPanelState = HydratorPlusPlusBottomPanelStore.getPanelState();
    };
    this.setIsCollapsed();
    this.selectTab = function(tab) {
      this.activeTab = this.tabs[tab];
      let bottomPanelState = HydratorPlusPlusBottomPanelStore.getPanelState();
      if (bottomPanelState === 0) {
        HydratorPlusPlusBottomPanelActions.expand();
      }
    };
    this.selectTab(0);

    this.toggleCollapse = function(expanded) {
      if(expanded) {
        HydratorPlusPlusBottomPanelActions.collapse();
      } else {
        HydratorPlusPlusBottomPanelActions.expand();
      }
    };
    this.toggleMaximized = function(maximized) {
      if (maximized !== 2) {
        HydratorPlusPlusBottomPanelActions.maximize();
      } else {
        HydratorPlusPlusBottomPanelActions.expand();
      }
    };
    HydratorPlusPlusBottomPanelStore.registerOnChangeListener(this.setIsCollapsed.bind(this));
    HydratorPlusPlusBottomPanelActions.expand();

    $scope.$on('$destroy', function() {
      HydratorPlusPlusBottomPanelActions.reset();
    });
  });
