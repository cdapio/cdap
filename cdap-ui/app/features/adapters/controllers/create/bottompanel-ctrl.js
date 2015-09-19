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

angular.module(PKG.name + '.feature.adapters')
  .controller('BottomPanelController', function ($scope, MySidebarService, MyAppDAGService, MyNodeConfigService, $timeout, MyConsoleTabService) {

    MyAppDAGService.registerEditPropertiesCallback(editProperties.bind(this));
    MyConsoleTabService.registerOnMessageUpdates(showConsoleTab.bind(this));
    MyAppDAGService.errorCallback(showConsoleTab.bind(this));
    // FIXME: We should be able to remove this now.
    // Expand and collapse of the sidebar resizes the main container natively.
    MySidebarService.registerIsExpandedCallback(isExpanded.bind(this));

    function showConsoleTab(errors) {
      if (errors.canvas && errors.canvas.length) {
        errors.canvas.forEach(function(err) {
          MyConsoleTabService.addMessage({
            type: 'error',
            content: err
          });
        });
      }
      $scope.selectTab($scope.tabs[0]);
    }

    function editProperties(plugin) {
      $scope.selectTab($scope.tabs[2], false);
      // Giving 100ms to load the template and then set the plugin
      // For this service to work the controller has to register a callback
      // with the service. The callback will not be called if plugin assignment happens
      // before controller initialization. Hence the 100ms delay.
      $timeout(function() {
        MyNodeConfigService.setPlugin(plugin);
      }, 100);
    }

    $scope.isExpanded = false;

    function isExpanded(value) {
      $scope.isExpanded = !value;
    }

    $scope.tabs = [
      {
        title: 'Console',
        template: '/assets/features/adapters/templates/partial/console.html'
      },
      {
        title: 'Pipeline Configuration',
        template: '/assets/features/adapters/templates/partial/settings.html'
      },
      {
        title: 'Node Configuration',
        template: '/assets/features/adapters/templates/partial/node-config.html'
      },
      {
        title: 'Reference',
        template: '/assets/features/adapters/templates/partial/reference-tab.html'
      }
    ];

    $scope.activeTab = $scope.tabs[0];

    $scope.selectTab = function(tab) {
      $scope.activeTab = tab;
    };
});
