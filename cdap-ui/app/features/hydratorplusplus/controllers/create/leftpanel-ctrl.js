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

class HydratorPlusPlusLeftPanelCtrl {
  constructor($scope, $stateParams, rVersion, GLOBALS, HydratorPlusPlusLeftPanelStore, HydratorPlusPlusPluginActions, HydratorPlusPlusConfigStore, HydratorPlusPlusConfigActions, DAGPlusPlusFactory, DAGPlusPlusNodesActionsFactory, NonStorePipelineErrorFactory, $uibModal, myAlertOnValium, $state, $q, rArtifacts, $timeout, PluginTemplatesDirActions, HydratorPlusPlusOrderingFactory) {
    this.$state = $state;
    this.$scope = $scope;
    this.$stateParams = $stateParams;
    this.HydratorPlusPlusLeftPanelStore = HydratorPlusPlusLeftPanelStore;
    this.HydratorPlusPlusPluginActions = HydratorPlusPlusPluginActions;
    this.HydratorPlusPlusConfigActions = HydratorPlusPlusConfigActions;
    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.DAGPlusPlusFactory = DAGPlusPlusFactory;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;
    this.PluginTemplatesDirActions = PluginTemplatesDirActions;
    this.rVersion = rVersion;
    this.$timeout = $timeout;
    this.myAlertOnValium = myAlertOnValium;
    this.$q = $q;
    this.HydratorPlusPlusOrderingFactory = HydratorPlusPlusOrderingFactory;

    this.pluginsMap = [];
    this.sourcesToVersionMap = {};
    this.transformsToVersionMap = {};
    this.sinksToVersionMap = {};

    this.artifacts = rArtifacts;
    let configStoreArtifact = this.HydratorPlusPlusConfigStore.getArtifact();
    this.selectedArtifact = rArtifacts.filter( ar => ar.name === configStoreArtifact.name)[0];
    this.artifactToRevert = this.selectedArtifact;
    this.HydratorPlusPlusPluginActions.fetchExtensions({
      namespace: $stateParams.namespace,
      pipelineType: this.selectedArtifact.name,
      version: this.rVersion.version,
      scope: this.$scope
    });

    this.HydratorPlusPlusPluginActions.fetchTemplates({namespace: this.$stateParams.namespace});
    this.HydratorPlusPlusLeftPanelStore.registerOnChangeListener( () => {
      let extensions = this.HydratorPlusPlusLeftPanelStore.getExtensions();
      if (!angular.isArray(extensions)) {
        return;
      }

      extensions.forEach( (ext) => {
        let isPluginAlreadyExist = (ext) => {
          return this.pluginsMap.filter( pluginObj => pluginObj.name === this.HydratorPlusPlusOrderingFactory.getPluginTypeDisplayName(ext));
        };
        if (!isPluginAlreadyExist(ext).length) {
          this.pluginsMap.push({
            name: this.HydratorPlusPlusOrderingFactory.getPluginTypeDisplayName(ext),
            plugins: []
          });
          let params = {
            namespace: this.$stateParams.namespace,
            pipelineType: this.HydratorPlusPlusConfigStore.getArtifact().name,
            version: this.rVersion.version,
            extensionType: ext,
            scope: this.$scope
          };
          this.HydratorPlusPlusPluginActions.fetchPlugins(ext, params);
        } else {
          this.pluginsMap
              .filter( pluginObj => pluginObj.name === this.HydratorPlusPlusOrderingFactory.getPluginTypeDisplayName(ext))
              .forEach( matchedObj => {
                let getPluginTemplateNode = (ext) => {
                  return this.HydratorPlusPlusLeftPanelStore.getPlugins(ext)
                         .concat(this.HydratorPlusPlusLeftPanelStore.getPluginTemplates(ext));
                };
                matchedObj.plugins = getPluginTemplateNode(ext);
              });
        }
      });
      this.pluginsMap = this.HydratorPlusPlusOrderingFactory.orderPluginTypes(this.pluginsMap);
    });

    this.$uibModal = $uibModal;
    this.$scope.$on('$destroy', () => {
      this.HydratorPlusPlusPluginActions.reset();
    });
  }

  onArtifactChange() {
    this._checkAndShowConfirmationModalOnDirtyState()
      .then(proceedToNextStep => {
        if (!proceedToNextStep) {
          this.selectedArtifact = this.artifactToRevert;
        } else {
          this.HydratorPlusPlusConfigStore.setState(this.HydratorPlusPlusConfigStore.getDefaults());
          this.$state.go('hydratorplusplus.create', {
            namespace: this.$state.params.namespace,
            artifactType: this.selectedArtifact.name,
            data: null,
          }, {reload: true, inherit: false});
        }
      });
  }

  openFileBrowser() {
    let fileBrowserClickCB = () => {
      document.getElementById('pipeline-import-config-link').click();
    };
    // This is not using the promise pattern as browsers NEED to have the click on the call stack to generate the click on input[type=file] button programmatically in like line:115.
    // When done in promise we go into the promise ticks and the then callback is called in the next tick which prevents the browser to open the file dialog
    // as a file dialog is opened ONLY when manually clicked by the user OR transferring the click to another button in the same call stack
    // TL;DR Can't open file dialog programmatically. If we need to, we need to transfer the click from a user on a button directly into the input file dialog button.
    this._checkAndShowConfirmationModalOnDirtyState(fileBrowserClickCB);
  }

  importFile(files) {
    if (files[0].name.indexOf('.json') === -1) {
      this.myAlertOnValium.show({
        type: 'danger',
        content: 'Pipeline configuration should be JSON.'
      });
      return;
    }
    let generateLinearConnections = (config) => {
      let nodes = [config.source].concat(config.transforms || []).concat(config.sinks);
      let connections = [];
      let i;
      for (i=0; i<nodes.length - 1 ; i++) {
        connections.push({ from: nodes[i].name, to: nodes[i+1].name });
      }
      return connections;
    };

    let isValidArtifact = (importArtifact) => {
      let isVersionExists = [];
      let isScopeExists = [];
      let isNameExists = this.artifacts.filter( artifact => artifact.name === importArtifact.name );
      isVersionExists = isNameExists.filter( artifact => artifact.version === importArtifact.version );
      isScopeExists = isNameExists.filter( artifact => artifact.scope === importArtifact.scope );
      return {
        name: isNameExists.length > 0,
        version: isVersionExists.length > 0,
        scope: isScopeExists.length > 0
      };
    };

    var reader = new FileReader();
    reader.readAsText(files[0], 'UTF-8');

    reader.onload =  (evt) => {
      var data = evt.target.result;
      var jsonData;
      try {
        jsonData = JSON.parse(data);
      } catch(e) {
        this.myAlertOnValium.show({
          type: 'danger',
          content: 'Syntax Error. Ill-formed pipeline configuration.'
        });
        return;
      }
      if (!jsonData.config.connections) {
        jsonData.config.connections = generateLinearConnections(jsonData.config);
      }

      let isNotValid = this.NonStorePipelineErrorFactory.validateImportJSON(jsonData);
      if (isNotValid) {
        this.myAlertOnValium.show({
          type: 'danger',
          content: isNotValid
        });
        return;
      }

      let validArtifact = isValidArtifact(jsonData.artifact);
      if (!validArtifact.name || !validArtifact.version || !validArtifact.scope) {
        let invalidFields = [];
        if (!validArtifact.name) {
          invalidFields.push('Artifact name');
        } else {
          if (!validArtifact.version) {
            invalidFields.push('Artifact version');
          }
          if (!validArtifact.scope) {
            invalidFields.push('Artifact scope');
          }
        }
        invalidFields = invalidFields.length === 1 ? invalidFields[0] : invalidFields.join(', ');
        this.myAlertOnValium.show({
          type: 'danger',
          content: `Imported pipeline has invalid artifact information: ${invalidFields}.`
        });
      } else {
        if (!jsonData.config.connections) {
          jsonData.config.connections = generateLinearConnections(jsonData.config);
        }
        this.HydratorPlusPlusConfigStore.setState(this.HydratorPlusPlusConfigStore.getDefaults());
        this.$state.go('hydratorplusplus.create', { data: jsonData });
      }
    };
  }

  showTemplates() {
    let templateType = this.HydratorPlusPlusConfigStore.getArtifact().name;
    let openTemplatesPopup = () => {
      this.$uibModal.open({
        templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/pre-configured-batch-list.html',
        size: 'lg',
        backdrop: true,
        keyboard: true,
        controller: 'HydratorPlusPlusPreConfiguredCtrl',
        controllerAs: 'HydratorPlusPlusPreConfiguredCtrl',
        windowTopClass: 'hydrator-template-modal hydrator-modal',
        resolve: {
          rTemplateType: () => templateType
        }
      });
    };
    this._checkAndShowConfirmationModalOnDirtyState()
      .then(proceedToNextStep =>{
        if (proceedToNextStep) {
          openTemplatesPopup();
        }
      });
  }

  _checkAndShowConfirmationModalOnDirtyState(proceedCb) {
    let goTonextStep = true;
    let isStoreDirty = this.HydratorPlusPlusConfigStore.getIsStateDirty();
    if (isStoreDirty) {
      return this.$uibModal.open({
        templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/canvas-overwrite-confirmation.html',
        size: 'lg',
        backdrop: 'static',
        keyboard: false,
        windowTopClass: 'confirm-modal hydrator-modal',
        controller: ['$scope', 'HydratorPlusPlusConfigStore', 'HydratorPlusPlusConfigActions', function($scope, HydratorPlusPlusConfigStore, HydratorPlusPlusConfigActions) {
          $scope.isSaving = false;
          $scope.discard = () => {
            goTonextStep = true;
            if (proceedCb) {
              proceedCb();
            }
            $scope.$close();
          };
          $scope.save = () => {
            let pipelineName = HydratorPlusPlusConfigStore.getName();
            if (!pipelineName.length) {
              HydratorPlusPlusConfigActions.saveAsDraft();
              goTonextStep = false;
              $scope.$close();
              return;
            }
            var unsub = HydratorPlusPlusConfigStore.registerOnChangeListener( () => {
              let isStateDirty = HydratorPlusPlusConfigStore.getIsStateDirty();
              // This is solely used for showing the spinner icon until the modal is closed.
              if(!isStateDirty) {
                unsub();
                goTonextStep = true;
                $scope.$close();
              }
            });
            HydratorPlusPlusConfigActions.saveAsDraft();
            $scope.isSaving = true;
          };
          $scope.cancel = () => {
            $scope.$close();
            goTonextStep = false;
          };
        }]
      })
      .closed
      .then(() => {
        return goTonextStep;
      });
    } else {
      if (proceedCb) {
        proceedCb();
      }
      return this.$q.when(goTonextStep);
    }
  }
  onLeftSidePanelItemClicked(event, node) {
    event.stopPropagation();
    if (node.action === 'createTemplate') {
      this.createPluginTemplate(node.contentData, 'create');
    } else if(node.action === 'deleteTemplate') {
      this.deletePluginTemplate(node.contentData);
    } else if(node.action === 'editTemplate') {
      this.createPluginTemplate(node.contentData, 'edit');
    } else {
      this.addPluginToCanvas(event, node);
    }
  }

  deletePluginTemplate(node) {
    this.$uibModal
      .open({
        templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/plugin-delete-confirmation.html',
        size: 'lg',
        backdrop: 'static',
        keyboard: false,
        windowTopClass: 'confirm-modal hydrator-modal',
        controller: 'PluginTemplatesDeleteCtrl',
        resolve: {
          rNode: () => node
        }
      });
  }

  createPluginTemplate(node, mode) {
    this.$uibModal
      .open({
        templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/plugin-templates.html',
        size: 'lg',
        backdrop: 'static',
        keyboard: false,
        windowTopClass: 'plugin-templates-modal hydrator-modal',
        controller: 'PluginTemplatesCreateEditCtrl'
      })
      .rendered
      .then(() => {
        this.PluginTemplatesDirActions.init({
          templateType: node.templateType || this.selectedArtifact.name,
          pluginType: node.pluginType || node.type,
          mode: mode === 'edit'? 'edit': 'create',
          templateName: node.pluginTemplate,
          pluginName: node.pluginName || node.name
        });
      });
  }
  addPluginToCanvas(event, node) {
    var item = this.HydratorPlusPlusLeftPanelStore.getSpecificPluginVersion(node);
    this.DAGPlusPlusNodesActionsFactory.resetSelectedNode();
    this.HydratorPlusPlusLeftPanelStore.updatePluginDefaultVersion(item);

    let name = item.name || item.pluginTemplate;

    let filteredNodes = this.HydratorPlusPlusConfigStore
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
        icon: this.DAGPlusPlusFactory.getIcon(item.pluginName),
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
    this.DAGPlusPlusNodesActionsFactory.addNode(config);
  }

  loadArtifact() {
    this.$uibModal
      .open({
        templateUrl: '/assets/features/hydratorplusplus/templates/create/popovers/load-artifact.html',
        size: 'lg',
        backdrop: 'static',
        keyboard: false,
        windowTopClass: 'load-artifact-modal hydrator-modal',
        controller: 'LoadArtifactCtrl',
        controllerAs: 'LoadArtifactCtrl'
      });
  }
}

HydratorPlusPlusLeftPanelCtrl.$inject = ['$scope', '$stateParams', 'rVersion', 'GLOBALS', 'HydratorPlusPlusLeftPanelStore', 'HydratorPlusPlusPluginActions', 'HydratorPlusPlusConfigStore', 'HydratorPlusPlusConfigActions', 'DAGPlusPlusFactory', 'DAGPlusPlusNodesActionsFactory', 'NonStorePipelineErrorFactory',  '$uibModal', 'myAlertOnValium', '$state', '$q', 'rArtifacts', '$timeout', 'PluginTemplatesDirActions', 'HydratorPlusPlusOrderingFactory'];
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusLeftPanelCtrl', HydratorPlusPlusLeftPanelCtrl);
