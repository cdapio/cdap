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

class BottomPanelController {
  constructor(PipelineDetailBottomPanelActionFactory, BottomPanelStore, NodeConfigStore, ConsoleStore) {
    this.tabs = [
      {
        title: 'Console',
        template: '/assets/features/hydrator/templates/partial/console.html'
      },
      {
        title: 'Pipeline Configuration',
        template: '/assets/features/hydrator/templates/partial/settings.html'
      },
      {
        title: 'Node Configuration',
        template: '/assets/features/hydrator/templates/partial/node-config.html'
      },
      {
        title: 'Reference',
        template: '/assets/features/hydrator/templates/partial/reference-tab.html'
      }
    ];
    this.PipelineDetailBottomPanelActionFactory = PipelineDetailBottomPanelActionFactory;
    this.NodeConfigStore = NodeConfigStore;
    this.BottomPanelStore = BottomPanelStore;

    this.BottomPanelStore.registerOnChangeListener(this.setIsCollapsed.bind(this));
    NodeConfigStore.registerOnChangeListener( () => {
      if (this.NodeConfigStore.getState().isValidPlugin) {
        this.selectTab(2);
      }
    } );
    ConsoleStore.registerOnChangeListener( () => { this.selectTab(0); } );
    this.setIsCollapsed();
    this.selectTab(0);
  }
  selectTab(tab) {
    this.activeTab = this.tabs[tab];
  }
  setIsCollapsed() {
    this.bottomPanelState = this.BottomPanelStore.getPanelState();
  }
  toggleCollapse(expanded) {
    if(expanded) {
      this.PipelineDetailBottomPanelActionFactory.collapse();
    } else {
      this.PipelineDetailBottomPanelActionFactory.expand();
    }
  }
  toggleMaximized(maximized) {
    if (maximized !== 2) {
      this.PipelineDetailBottomPanelActionFactory.maximize();
    } else {
      this.PipelineDetailBottomPanelActionFactory.expand();
    }
  }
}

BottomPanelController.$inject = ['PipelineDetailBottomPanelActionFactory', 'BottomPanelStore', 'NodeConfigStore', 'ConsoleStore'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('BottomPanelController', BottomPanelController);
