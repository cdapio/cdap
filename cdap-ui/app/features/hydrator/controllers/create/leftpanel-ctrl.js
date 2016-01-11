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

class LeftPanelController {
  constructor($scope, $stateParams, rVersion, GLOBALS, LeftPanelStore, LeftPanelActionsFactory, PluginActionsFactory, ConfigStore, ConfigActionsFactory, MyDAGFactory, NodesActionsFactory, NonStorePipelineErrorFactory, HydratorService) {
    this.$scope = $scope;
    this.$stateParams = $stateParams;
    this.LeftPanelStore = LeftPanelStore;
    this.LeftPanelActionsFactory = LeftPanelActionsFactory;
    this.PluginActionsFactory = PluginActionsFactory;
    this.ConfigActionsFactory = ConfigActionsFactory;
    this.GLOBALS = GLOBALS;
    this.ConfigStore = ConfigStore;
    this.MyDAGFactory = MyDAGFactory;
    this.NodesActionsFactory = NodesActionsFactory;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;
    this.HydratorService = HydratorService;

    this.pluginTypes = [
      {
        name: 'source',
        expanded: false,
        plugins: []
      },
      {
        name: 'transform',
        expanded: false,
        plugins: []
      },
      {
        name: 'sink',
        expanded: false,
        plugins: []
      }
    ];
    this.sourcesToVersionMap = {};
    this.transformsToVersionMap = {};
    this.sinksToVersionMap = {};

    this.LeftPanelStore.registerOnChangeListener(() => {
      this.pluginTypes[0].plugins = this.LeftPanelStore.getSources();
      this.pluginTypes[1].plugins = this.LeftPanelStore.getTransforms();
      this.pluginTypes[2].plugins = this.LeftPanelStore.getSinks();
    });

    let params = {
      namespace: this.$stateParams.namespace,
      pipelineType: ConfigStore.getArtifact().name,
      version: rVersion.version,
      scope: this.$scope
    };
    this.PluginActionsFactory.fetchSources(params);
    this.PluginActionsFactory.fetchTransforms(params);
    this.PluginActionsFactory.fetchSinks(params);

  }

  onLeftSidePanelItemClicked(event, node) {
    event.stopPropagation();
    var item = this.LeftPanelStore.getSpecificPluginVersion(node);
    this.LeftPanelStore.updatePluginDefaultVersion(node);
    let filteredNodes = this.ConfigStore
                    .getNodes()
                    .filter( node => node.plugin.label.includes(item.name) );
    let config;
    if (item.pluginTemplate) {
      config = {
        plugin: {
          label: (filteredNodes.length > 0 ? item.name + (filteredNodes.length+1): item.name),
          name: item.pluginName,
          artifact: item.artifact,
          properties: item.properties,
        },
        icon: this.MyDAGFactory.getIcon(item.pluginName),
        type: item.pluginType,
        outputSchema: item.outputSchema,
        inputSchema: item.inputSchema,
        pluginTemplate: item.pluginTemplate,
        lock: item.lock
      };
    } else {
      config = {
        plugin: {
          label: (filteredNodes.length > 0 ? item.name + (filteredNodes.length+1): item.name),
          artifact: item.artifact,
          name: item.name,
          properties: {}
        },
        icon: item.icon,
        description: item.description,
        type: item.type,
        warning: true
      };
    }
    this.NodesActionsFactory.addNode(config);
  }
}

LeftPanelController.$inject = ['$scope', '$stateParams', 'rVersion', 'GLOBALS', 'LeftPanelStore', 'LeftPanelActionsFactory', 'PluginActionsFactory', 'ConfigStore', 'ConfigActionsFactory', 'MyDAGFactory', 'NodesActionsFactory', 'NonStorePipelineErrorFactory', 'HydratorService'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('LeftPanelController', LeftPanelController);
