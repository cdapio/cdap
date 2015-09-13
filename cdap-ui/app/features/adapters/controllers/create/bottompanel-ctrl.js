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
  .controller('BottomPanelController', function ($scope, MySidebarService, MyAppDAGService) {

    MyAppDAGService.registerEditPropertiesCallback(editProperties.bind(this));
    this.tab = {};
    this.tab.plugin = {};
    function editProperties(plugin) {
      this.tab.plugin = plugin;
      $scope.selectTab($scope.tabs[2]);
    }

    $scope.isCollapsed = false;
    $scope.collapseToggle = function() {
      $scope.isCollapsed = !$scope.isCollapsed;
    };
    $scope.isExpanded = false;

    function isExpanded(value) {
      $scope.isExpanded = !value;
    }

    MySidebarService.registerIsExpandedCallback(isExpanded.bind(this));

    $scope.tabs = [
      {
        title: 'Console',
        template: '/assets/features/adapters/templates/partial/console.html'
      },
      {
        title: 'Pipeline Configuration',
        template: '/assets/features/adapters/templates/partial/pipeline-config.html'
      },
      {
        title: 'Node Configuration',
        template: '/assets/features/adapters/templates/partial/node-config.html'
      },
      {
        title: 'Reference',
        template: '/assets/features/adapters/templates/partial/reference.html'
      }
    ];

    $scope.activeTab = $scope.tabs[0];

    $scope.selectTab = function(tab) {
      $scope.activeTab = tab;
    };
});
