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

class TopPanelController{
  constructor(GLOBALS, $stateParams, $alert, ConfigStore, ConfigActionsFactory, $bootstrapModal, ConsoleActionsFactory, NodesActionsFactory) {
    this.GLOBALS = GLOBALS;
    this.ConfigStore = ConfigStore;
    this.ConfigActionsFactory = ConfigActionsFactory;
    this.$bootstrapModal = $bootstrapModal;
    this.ConsoleActionsFactory = ConsoleActionsFactory;
    this.NodesActionsFactory = NodesActionsFactory;
    this.parsedDescription = this.ConfigStore.getDescription();

    this.canvasOperations = [
      {
        name: 'Export',
        icon: 'icon-export',
        fn: this.onExport.bind(this)
      },
      {
        name: 'Save',
        icon: 'icon-savedraft',
        fn: this.onSaveDraft.bind(this)
      },
      {
        name: 'Validate',
        icon: 'icon-validate',
        fn: this.onValidate.bind(this)
      },
      {
        name: 'Publish',
        icon: 'icon-publish',
        fn: this.onPublish.bind(this)
      }
    ];
    this.$stateParams = $stateParams;
    this.setState();
    ConfigStore.registerOnChangeListener(this.setState.bind(this));
  }
  setMetadata(metadata) {
    this.state.metadata = metadata;
  }
  setArtifact(artifact) {
    this.state.artifact = artifact;
  }
  setState() {
    this.state = {
      metadata: {
        name: this.ConfigStore.getName(),
        description: this.ConfigStore.getDescription()
      },
      artifact: this.ConfigStore.getArtifact()
    };
  }

  openMetadata() {
    this.metadataExpanded = true;
  }
  resetMetadata() {
    this.setState();
    this.metadataExpanded = false;
  }
  saveMetadata() {
    this.ConfigActionsFactory.setMetadataInfo(this.state.metadata.name, this.state.metadata.description);
    if (this.state.metadata.description) {
      this.parsedDescription = this.state.metadata.description.replace(/\n/g, ' ');
      this.tooltipDescription = this.state.metadata.description.replace(/\n/g, '<br />');
    } else {
      this.parsedDescription = '';
      this.tooltipDescription = '';
    }
    this.metadataExpanded = false;
  }
  onEnterOnMetadata(event) {
    // Save when user hits ENTER key.
    if (event.keyCode === 13) {
      this.saveMetadata();
      this.metadataExpanded = false;
    } else if (event.keyCode === 27) {
      // Reset if the user hits ESC key.
      this.resetMetadata();
    }
  }

  onExport() {
    this.NodesActionsFactory.resetSelectedNode();
    let config = angular.copy(this.ConfigStore.getDisplayConfig());
    if (!config) {
      return;
    }
    this.$bootstrapModal.open({
      templateUrl: '/assets/features/hydrator/templates/create/popovers/viewconfig.html',
      size: 'lg',
      keyboard: true,
      controller: ['$scope', 'config', '$timeout', 'exportConfig', function($scope, config, $timeout, exportConfig) {
        $scope.config = JSON.stringify(config);
        $scope.export = function () {
          var blob = new Blob([JSON.stringify(exportConfig, null, 4)], { type: 'application/json'});
          $scope.url = URL.createObjectURL(blob);
          $scope.exportFileName = (exportConfig.name? exportConfig.name: 'noname') + '-' + exportConfig.artifact.name;
          $scope.$on('$destroy', function () {
            URL.revokeObjectURL($scope.url);
          });
          $timeout(function() {
            document.getElementById('pipeline-export-config-link').click();
          });
        };
      }],
      resolve: {
        config: () => config,
        exportConfig: () => this.ConfigStore.getConfigForExport()
      }
    });
  }
  onSaveDraft() {
    var config = this.ConfigStore.getState();
    if (!config.name) {
      this.ConsoleActionsFactory.addMessage({
        type: 'error',
        content: this.GLOBALS.en.hydrator.studio.error['MISSING-NAME']
      });
      return;
    }
    this.ConfigActionsFactory.saveAsDraft(config);
  }
  onValidate() {
    this.ConsoleActionsFactory.resetMessages();
    let isStateValid = this.ConfigStore.validateState(true);
    if (isStateValid) {
      this.ConsoleActionsFactory.addMessage({
        type: 'success',
        content: 'Validation success! Pipeline ' + this.ConfigStore.getName() + ' is valid.'
      });
    }
  }
  onPublish() {
    this.ConfigActionsFactory.publishPipeline();
  }
}

TopPanelController.$inject = ['GLOBALS', '$stateParams', '$alert', 'ConfigStore', 'ConfigActionsFactory', '$bootstrapModal', 'ConsoleActionsFactory', 'NodesActionsFactory'];

angular.module(PKG.name + '.feature.hydrator')
  .controller('TopPanelController', TopPanelController);
