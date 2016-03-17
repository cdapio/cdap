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

class LeftPanelControllerBeta {
  constructor($scope, $stateParams, rVersion, GLOBALS, LeftPanelStoreBeta, LeftPanelActionsFactoryBeta, PluginActionsFactoryBeta, ConfigStoreBeta, ConfigActionsFactoryBeta, MyDAGFactoryBeta, NodesActionsFactoryBeta, NonStorePipelineErrorFactory, HydratorServiceBeta, $rootScope, $uibModal) {
    this.$rootScope = $rootScope;
    this.$scope = $scope;
    this.$stateParams = $stateParams;
    this.LeftPanelStoreBeta = LeftPanelStoreBeta;
    this.LeftPanelActionsFactoryBeta = LeftPanelActionsFactoryBeta;
    this.PluginActionsFactoryBeta = PluginActionsFactoryBeta;
    this.ConfigActionsFactoryBeta = ConfigActionsFactoryBeta;
    this.GLOBALS = GLOBALS;
    this.ConfigStoreBeta = ConfigStoreBeta;
    this.MyDAGFactoryBeta = MyDAGFactoryBeta;
    this.NodesActionsFactoryBeta = NodesActionsFactoryBeta;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;
    this.HydratorServiceBeta = HydratorServiceBeta;
    this.rVersion = rVersion;

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

    this.LeftPanelStoreBeta.registerOnChangeListener(() => {
      this.artifacts = this.LeftPanelStoreBeta.getArtifacts();
      let artifactFromConfigStore = this.ConfigStoreBeta.getArtifact();
      if (!this.selectedArtifact) {
        if (artifactFromConfigStore.name.length) {
          this.selectedArtifact = this.artifacts.filter(artifact => angular.equals(artifact, artifactFromConfigStore))[0];
        } else {
          this.selectedArtifact =  this.artifacts[0];
        }
        this.ConfigActionsFactoryBeta.setArtifact(this.selectedArtifact);
        console.log('selectedArticact', this.selectedArtifact);
        this.onArtifactChange();
        return;
      }
      this.pluginTypes[0].plugins = this.LeftPanelStoreBeta.getSources();
      this.pluginTypes[1].plugins = this.LeftPanelStoreBeta.getTransforms();
      this.pluginTypes[2].plugins = this.LeftPanelStoreBeta.getSinks();
    });
    this.$uibModal = $uibModal;
    this.PluginActionsFactoryBeta.fetchArtifacts({namespace: $stateParams.namespace});
  }

  onArtifactChange() {
    console.log('On change selectedArticact', this.selectedArtifact);
    this.ConfigActionsFactoryBeta.setArtifact(this.selectedArtifact);
    let params = {
      namespace: this.$stateParams.namespace,
      pipelineType: this.ConfigStoreBeta.getArtifact().name,
      version: this.rVersion.version,
      scope: this.$scope
    };
    this.PluginActionsFactoryBeta.fetchSources(params);
    this.PluginActionsFactoryBeta.fetchTransforms(params);
    this.PluginActionsFactoryBeta.fetchSinks(params);
    this.PluginActionsFactoryBeta.fetchTemplates(params);

  }

  showTemplates() {
    let templateType = this.ConfigStoreBeta.getArtifact().name;
    this.$uibModal.open({
      templateUrl: '/assets/features/hydrator-beta/templates/create/popovers/pre-configured-batch-list.html',
      size: 'lg',
      backdrop: true,
      keyboard: true,
      controller: 'PreConfiguredControllerBeta',
      controllerAs: 'PreConfiguredControllerBeta',
      windowTopClass: 'hydrator-template-modal',
      resolve: {
        rTemplateType: () => templateType
      }
    });
  }
  onLeftSidePanelItemClicked(event, node) {
    event.stopPropagation();
    var item = this.LeftPanelStoreBeta.getSpecificPluginVersion(node);
    this.NodesActionsFactoryBeta.resetSelectedNode();
    this.LeftPanelStoreBeta.updatePluginDefaultVersion(item);

    let name = item.name || item.pluginTemplate;

    let filteredNodes = this.ConfigStoreBeta
                    .getNodes()
                    .filter( node => (node.plugin.label ? node.plugin.label.includes(name) : false) );
    let config;
    if (item.pluginTemplate) {
      config = {
        plugin: {
          label: (filteredNodes.length > 0 ? item.pluginTemplate + (filteredNodes.length+1): item.pluginTemplate),
          name: item.pluginName,
          artifact: item.artifact,
          properties: item.properties,
        },
        icon: this.MyDAGFactoryBeta.getIcon(item.pluginName),
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
    this.NodesActionsFactoryBeta.addNode(config);
  }
}

LeftPanelControllerBeta.$inject = ['$scope', '$stateParams', 'rVersion', 'GLOBALS', 'LeftPanelStoreBeta', 'LeftPanelActionsFactoryBeta', 'PluginActionsFactoryBeta', 'ConfigStoreBeta', 'ConfigActionsFactoryBeta', 'MyDAGFactoryBeta', 'NodesActionsFactoryBeta', 'NonStorePipelineErrorFactory', 'HydratorServiceBeta', '$rootScope', '$uibModal'];
angular.module(PKG.name + '.feature.hydrator-beta')
  .controller('LeftPanelControllerBeta', LeftPanelControllerBeta);
