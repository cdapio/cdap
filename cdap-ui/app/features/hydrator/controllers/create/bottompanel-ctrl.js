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
  .controller('BottomPanelController', function (BottomPanelStore, NodeConfigStore, PipelineDetailBottomPanelActionFactory, MyConsoleTabService, MyAppDAGService) {
    this.setIsCollapsed = function() {
      this.bottomPanelState = BottomPanelStore.getPanelState();
    };

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

    this.selectTab = function(tab) {
      this.activeTab = this.tabs[tab];
    };
    this.selectTab(0);
    this.setIsCollapsed();

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

    NodeConfigStore.registerOnChangeListener(function() {
      this.selectTab(2);
    }.bind(this));

    this.flashDanger = function() {
      this.dangerBg = true;
      setTimeout(function() {
        this.dangerBg = false;
      }.bind(this), 3000);
    };

    this.flashInfo = function() {
      this.infoBg = true;
      setTimeout(function() {
        this.infoBg = false;
      }.bind(this), 3000);
    };

    this.flashSuccess = function() {
      this.successBg = true;
      setTimeout(function() {
        this.successBg = false;
      }.bind(this), 3000);
    };

    function showConsoleTab(message) {
      switch(message.type) {
        case 'error':
          this.flashDanger();
          break;
        case 'info':
          this.flashInfo();
          break;
        case 'success':
          this.flashSuccess();
          break;
      }
      if (message.canvas && message.canvas.length) {
        this.messageCount = message.canvas.length;
        message.canvas.forEach(function(err) {
          // Super f*&^k up. If I am correct this will cause recurrsion problems. Cause: Line 116
          MyConsoleTabService.addMessage({
            type: 'error',
            content: err
          });
        });
      }
      PipelineDetailBottomPanelActionFactory.expand();
      this.selectTab(0);
    }

    MyAppDAGService.errorCallback(showConsoleTab.bind(this));
    MyConsoleTabService.registerOnMessageUpdates(showConsoleTab.bind(this));

  });
