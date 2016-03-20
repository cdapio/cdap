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

class BottomPanelControllerBeta {
  constructor(PipelineDetailBottomPanelActionFactoryBeta, BottomPanelStoreBeta, NodeConfigStoreBeta, ConsoleStoreBeta) {
    this.tabs = [
      {
        title: 'Console',
        template: '/assets/features/hydrator-beta/templates/partial/console.html'
      },
      {
        title: 'Pipeline Configuration',
        template: '/assets/features/hydrator-beta/templates/partial/settings.html'
      },
      {
        title: 'Node Configuration',
        template: '/assets/features/hydrator-beta/templates/partial/node-config.html'
      },
      {
        title: 'Reference',
        template: '/assets/features/hydrator-beta/templates/partial/reference-tab.html'
      }
    ];
    this.PipelineDetailBottomPanelActionFactoryBeta = PipelineDetailBottomPanelActionFactoryBeta;
    this.NodeConfigStoreBeta = NodeConfigStoreBeta;
    this.BottomPanelStoreBeta = BottomPanelStoreBeta;

    this.BottomPanelStoreBeta.registerOnChangeListener(this.setIsCollapsed.bind(this));
    NodeConfigStoreBeta.registerOnChangeListener( () => {
      if (this.NodeConfigStoreBeta.getState().isValidPlugin) {
        this.selectTab(2);
      }
    } );
    ConsoleStoreBeta.registerOnChangeListener( () => { this.selectTab(0); } );

    this.selectTab(0);
  }
  selectTab(tab) {
    this.activeTab = this.tabs[tab];
  }
  setIsCollapsed() {
    this.bottomPanelState = this.BottomPanelStoreBeta.getPanelState();
  }
  toggleCollapse(expanded) {
    if(expanded) {
      this.PipelineDetailBottomPanelActionFactoryBeta.collapse();
    } else {
      this.PipelineDetailBottomPanelActionFactoryBeta.expand();
    }
  }
  toggleMaximized(maximized) {
    if (maximized !== 2) {
      this.PipelineDetailBottomPanelActionFactoryBeta.maximize();
    } else {
      this.PipelineDetailBottomPanelActionFactoryBeta.expand();
    }
  }
}

BottomPanelControllerBeta.$inject = ['PipelineDetailBottomPanelActionFactoryBeta', 'BottomPanelStoreBeta', 'NodeConfigStoreBeta', 'ConsoleStoreBeta'];
angular.module(PKG.name + '.feature.hydrator-beta')
  .controller('BottomPanelControllerBeta', BottomPanelControllerBeta);
