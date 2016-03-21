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

class HydratorPlusPlusBottomPanelCtrl {
  constructor(HydratorPlusPlusBottomPanelActions, HydratorPlusPlusBottomPanelStore, HydratorPlusPlusNodeConfigStore, HydratorPlusPlusConsoleStore) {
    this.tabs = [
      {
        title: 'Console',
        template: '/assets/features/hydratorplusplus/templates/partial/console.html'
      },
      {
        title: 'Pipeline Configuration',
        template: '/assets/features/hydratorplusplus/templates/partial/settings.html'
      },
      {
        title: 'Node Configuration',
        template: '/assets/features/hydratorplusplus/templates/partial/node-config.html'
      },
      {
        title: 'Reference',
        template: '/assets/features/hydratorplusplus/templates/partial/reference-tab.html'
      }
    ];
    this.HydratorPlusPlusBottomPanelActions = HydratorPlusPlusBottomPanelActions;
    this.HydratorPlusPlusNodeConfigStore = HydratorPlusPlusNodeConfigStore;
    this.HydratorPlusPlusBottomPanelStore = HydratorPlusPlusBottomPanelStore;

    this.HydratorPlusPlusBottomPanelStore.registerOnChangeListener(this.setIsCollapsed.bind(this));
    HydratorPlusPlusNodeConfigStore.registerOnChangeListener( () => {
      if (this.HydratorPlusPlusNodeConfigStore.getState().isValidPlugin) {
        this.selectTab(2);
      }
    } );
    HydratorPlusPlusConsoleStore.registerOnChangeListener( () => { this.selectTab(0); } );

    this.selectTab(0);
  }
  selectTab(tab) {
    this.activeTab = this.tabs[tab];
  }
  setIsCollapsed() {
    this.bottomPanelState = this.HydratorPlusPlusBottomPanelStore.getPanelState();
  }
  toggleCollapse(expanded) {
    if(expanded) {
      this.HydratorPlusPlusBottomPanelActions.collapse();
    } else {
      this.HydratorPlusPlusBottomPanelActions.expand();
    }
  }
  toggleMaximized(maximized) {
    if (maximized !== 2) {
      this.HydratorPlusPlusBottomPanelActions.maximize();
    } else {
      this.HydratorPlusPlusBottomPanelActions.expand();
    }
  }
}

HydratorPlusPlusBottomPanelCtrl.$inject = ['HydratorPlusPlusBottomPanelActions', 'HydratorPlusPlusBottomPanelStore', 'HydratorPlusPlusNodeConfigStore', 'HydratorPlusPlusConsoleStore'];
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusBottomPanelCtrl', HydratorPlusPlusBottomPanelCtrl);
