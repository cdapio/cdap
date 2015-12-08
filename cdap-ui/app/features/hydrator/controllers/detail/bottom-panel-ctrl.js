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

angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorDetailBottomPanelController', function(BottomPanelStore, PipelineDetailBottomPanelActionFactory, NodeConfigStore, $scope) {
    this.tabs = [
      {
        title: 'History',
        template: '/assets/features/hydrator/templates/detail/tabs/history.html'
      },
      {
        title: 'Log',
        template: '/assets/features/hydrator/templates/detail/tabs/log.html'
      },
      {
        title: 'Metrics',
        template: '/assets/features/hydrator/templates/detail/tabs/metrics.html'
      },
      {
        title: 'Configuration',
        template: '/assets/features/hydrator/templates/detail/tabs/configuration.html'
      },
      {
        title: 'Datasets',
        template: '/assets/features/hydrator/templates/detail/tabs/datasets.html'
      },
      {
        title: 'Node Configuration',
        template: '/assets/features/hydrator/templates/detail/tabs/node-configuration.html'
      }
    ];

    this.setIsCollapsed = function() {
      this.bottomPanelState = BottomPanelStore.getPanelState();
    };
    this.setIsCollapsed();
    this.selectTab = function(tab) {
      this.activeTab = this.tabs[tab];
      PipelineDetailBottomPanelActionFactory.expand();
    };
    this.selectTab(0);

    this.toggleCollapse = function(expanded) {
      if(expanded) {
        PipelineDetailBottomPanelActionFactory.collapse();
      } else {
        PipelineDetailBottomPanelActionFactory.expand();
      }
    };
    this.toggleMaximized = function(maximized) {
      if (maximized !== 2) {
        PipelineDetailBottomPanelActionFactory.maximize();
      } else {
        PipelineDetailBottomPanelActionFactory.expand();
      }
    };
    BottomPanelStore.registerOnChangeListener(this.setIsCollapsed.bind(this));
    PipelineDetailBottomPanelActionFactory.expand();
    NodeConfigStore.registerOnChangeListener(function() {
      this.selectTab(5);
    }.bind(this));

    $scope.$on('$destroy', function() {
      PipelineDetailBottomPanelActionFactory.reset();
    });
  });
