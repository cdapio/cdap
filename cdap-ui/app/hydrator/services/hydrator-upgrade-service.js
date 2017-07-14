/*
 * Copyright © 2017 Cask Data, Inc.
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

class HydratorUpgradeService {
  constructor($rootScope, myPipelineApi, $state, $uibModal, HydratorPlusPlusConfigStore, HydratorPlusPlusLeftPanelStore, $q) {
    this.$rootScope = $rootScope;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;
    this.$uibModal = $uibModal;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.leftPanelStore = HydratorPlusPlusLeftPanelStore;
    this.$q = $q;
  }

  _checkVersionIsInRange(range, version) {
    if (!range || !version) { return false; }

    if (['[', '('].indexOf(range[0]) !== -1) {
      const supportedVersion = new window.CaskCommon.Version(version);
      const versionRange = new window.CaskCommon.VersionRange(range);

      return versionRange.versionIsInRange(supportedVersion);
    }

    // Check equality if range is just a single version
    return range === version;
  }

  checkPipelineArtifactVersion(config) {
    if (!config || !config.artifact) { return false; }

    let cdapVersion = this.$rootScope.cdapVersion;

    return this._checkVersionIsInRange(config.artifact.version, cdapVersion);
  }

  _fetchPostRunActions() {
    let params = {
      namespace: this.$state.params.namespace,
      pipelineType: 'cdap-data-pipeline',
      version: this.$rootScope.cdapVersion,
      extensionType: 'postaction'
    };

    return this.myPipelineApi.fetchPlugins(params);
  }

  /**
   * Create plugin artifacts map based on left panel store.
   * The key will be '<plugin name>-<plugin type>-<artifact name>'
   * Each map will contain an array of all the artifacts and
   * also information about highest version.
   * If there exist 2 artifacts with same version, it will maintain both scopes in an array.
   **/
  _createPluginsMap(pipelineConfig) {
    let plugins = this.leftPanelStore.getState().plugins.pluginTypes;
    let pluginTypes = Object.keys(plugins);

    let pluginsMap = {};

    pluginTypes.forEach((type) => {
      plugins[type].forEach((plugin) => {
        let key = `${plugin.name}-${type}-${plugin.artifact.name}`;

        let allArtifacts = plugin.allArtifacts.map((artifactInfo) => {
          return artifactInfo.artifact;
        });

        let highestVersion;
        let artifactVersionMap = {};

        allArtifacts.forEach((artifact) => {
          if (!highestVersion) {
            highestVersion = artifact;
          } else if (highestVersion.version === artifact.version) {
            highestVersion.scope = [highestVersion.scope, artifact.scope];
          } else {
            let prevVersion = new window.CaskCommon.Version(highestVersion.version);
            let currVersion = new window.CaskCommon.Version(artifact.version);

            if (currVersion.compareTo(prevVersion) === 1) {
              highestVersion = artifact;
            }
          }

          if (artifactVersionMap[artifact.version]) {
            let existingScope = artifactVersionMap[artifact.version];
            artifactVersionMap[artifact.version] = [existingScope, artifact.scope];
          } else {
            artifactVersionMap[artifact.version] = artifact.scope;
          }
        });

        let value = {
          allArtifacts,
          highestVersion,
          artifactVersionMap
        };

        pluginsMap[key] = value;
      });
    });

    let deferred = this.$q.defer();

    if (pipelineConfig.artifact.name === 'cdap-data-pipeline') {
      this._fetchPostRunActions()
        .$promise
        .then((res) => {
          let postRunActionsMap = {};

          res.forEach((plugin) => {
            let postRunKey = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;

            postRunActionsMap[postRunKey] = {
              allArtifacts: [plugin.artifact],
              highestVersion: plugin.artifact,
              artifactVersionMap: {}
            };

            postRunActionsMap[postRunKey].artifactVersionMap[plugin.artifact.version] = plugin.artifact.scope;
          });

          pluginsMap  = Object.assign(pluginsMap, postRunActionsMap);

          deferred.resolve(pluginsMap);
        });
    } else {
      deferred.resolve(pluginsMap);
    }

    return deferred.promise;
  }

  _checkErrorStages(stages, pluginsMap) {
    let transformedStages = [];

    stages.forEach((stage) => {
      let stageKey = `${stage.plugin.name}-${stage.plugin.type}-${stage.plugin.artifact.name}`;

      let stageArtifact = stage.plugin.artifact;

      let data = {
        stageInfo: stage,
        error: null
      };

      if (!pluginsMap[stageKey]) {
        data.error = 'NOTFOUND';
      } else if (!this._checkVersionIsInRange(stageArtifact.version, pluginsMap[stageKey].highestVersion.version)) {
        data.error = 'VERSION_MISMATCH';
        data.suggestion = pluginsMap[stageKey].highestVersion;

        if (typeof data.suggestion.scope !== 'string') {
          // defaulting to USER scope when both version exists
          data.suggestion.scope = 'USER';
        }

        // This is to check whether the version of the imported pipeline exist or not
        let existingVersion = pluginsMap[stageKey].artifactVersionMap[stageArtifact.version];
        if (existingVersion && existingVersion.indexOf(stageArtifact.scope) !== -1) {
          data.error = 'CAN_UPGRADE';
        }
      } else if (pluginsMap[stageKey].highestVersion.scope.indexOf(stageArtifact.scope) < 0) {
        data.error = 'SCOPE_MISMATCH';
        data.suggestion = pluginsMap[stageKey].highestVersion;
      }

      transformedStages.push(data);
    });

    return transformedStages;
  }

  getErrorStages(pipelineConfig) {
    let configStages = pipelineConfig.config.stages;
    let configPostActions = pipelineConfig.config.postActions;

    return this._createPluginsMap(pipelineConfig)
      .then((pluginsMap) => {
        let stages = this._checkErrorStages(configStages, pluginsMap);
        let postActions = this._checkErrorStages(configPostActions, pluginsMap);

        return {
          stages,
          postActions
        };
      });
  }

  upgradePipelineArtifactVersion(pipelineConfig) {
    if (!pipelineConfig || !pipelineConfig.artifact) { return; }

    let cdapVersion = this.$rootScope.cdapVersion;

    let configClone = _.cloneDeep(pipelineConfig);

    configClone.artifact.version = cdapVersion;

    return configClone;
  }

  validateAndUpgradeConfig(pipelineConfig) {
    this.$uibModal.open({
      templateUrl: '/assets/features/hydrator/templates/create/pipeline-upgrade-modal.html',
      size: 'lg',
      backdrop: 'static',
      keyboard: false,
      windowTopClass: 'hydrator-modal node-config-modal upgrade-modal',
      controllerAs: 'PipelineUpgradeController',
      controller: function ($scope, rPipelineConfig, HydratorUpgradeService, $rootScope, HydratorPlusPlusConfigStore, $state, DAGPlusPlusFactory, GLOBALS, HydratorPlusPlusLeftPanelStore) {
        let eventEmitter = window.CaskCommon.ee(window.CaskCommon.ee);
        let globalEvents = window.CaskCommon.globalEvents;

        this.pipelineConfig = rPipelineConfig;
        this.cdapVersion = $rootScope.cdapVersion;

        this.pipelineArtifact = HydratorUpgradeService.checkPipelineArtifactVersion(rPipelineConfig);

        this.problematicStages = [];
        this.missingArtifactStages = [];
        this.canUpgradeStages = [];
        let allStages = [];

        let allPostActions = [];
        this.problematicPostRunActions = [];
        this.missingArtifactPostRunActions = [];
        this.fixAllDisabled = true;

        this.loading = true;

        const checkStages = () => {
          this.loading = true;

          HydratorUpgradeService.getErrorStages(rPipelineConfig)
            .then((transformedStages) => {
              allStages = transformedStages.stages.map((stage) => {
                stage.icon = DAGPlusPlusFactory.getIcon(stage.stageInfo.plugin.name.toLowerCase());
                stage.type = GLOBALS.pluginConvert[stage.stageInfo.plugin.type];
                return stage;
              });

              allPostActions = transformedStages.postActions.map((stage) => {
                stage.icon = DAGPlusPlusFactory.getIcon(stage.stageInfo.plugin.name.toLowerCase());
                stage.type = 'postaction';
                return stage;
              });

              this.problematicStages = [];
              this.missingArtifactStages = [];
              this.canUpgradeStages = [];
              this.problematicPostRunActions = [];
              this.missingArtifactPostRunActions = [];

              transformedStages.stages.forEach((artifact) => {
                if (artifact.error === 'NOTFOUND') {
                  this.missingArtifactStages.push(artifact);
                } else if (artifact.error === 'CAN_UPGRADE') {
                  artifact.upgrade = true;
                  this.canUpgradeStages.push(artifact);
                } else if (artifact.error) {
                  this.problematicStages.push(artifact);
                }
              });

              transformedStages.postActions.forEach((artifact) => {
                if (artifact.error === 'NOTFOUND') {
                  this.missingArtifactPostRunActions.push(artifact);
                } else if (artifact.error) {
                  this.problematicPostRunActions.push(artifact);
                }
              });

              this.fixAllDisabled = this.missingArtifactStages.length > 0 || this.missingArtifactPostRunActions.length > 0;

              if (
                this.problematicStages.length === 0 &&
                this.missingArtifactStages.length === 0 &&
                this.pipelineArtifact &&
                this.canUpgradeStages.length === 0 &&
                this.missingArtifactPostRunActions.length === 0 &&
                this.problematicPostRunActions.length === 0
              ) {
                HydratorPlusPlusConfigStore.setState(HydratorPlusPlusConfigStore.getDefaults());
                $state.go('hydrator.create', { data: rPipelineConfig });
              } else {
                this.loading = false;
              }
            });
        };

        checkStages();

        let sub = HydratorPlusPlusLeftPanelStore.subscribe(checkStages);

        this.openMarket = () => {
          eventEmitter.emit(globalEvents.OPENMARKET);
        };

        const fix = (stagesList) => {
          return stagesList.map((stage) => {
            let updatedStageInfo = stage.stageInfo;

            if (stage.error && stage.error === 'NOTFOUND') {
              updatedStageInfo.error = true;
              updatedStageInfo.errorCount = 1;
              updatedStageInfo.errorMessage = 'Plugin cannot be found';
            } else if (stage.error) {
              if (stage.error === 'CAN_UPGRADE' && stage.upgrade || stage.error !== 'CAN_UPGRADE') {
                updatedStageInfo.plugin.artifact = stage.suggestion;
              }
            }

            return updatedStageInfo;
          });
        };

        this.fixAll = () => {
          let newConfig = HydratorUpgradeService.upgradePipelineArtifactVersion(rPipelineConfig);

          // Making a copy here so that the information in the modal does not change when
          // we modify the artifact information
          let copyAllStages = angular.copy(allStages);
          let copyPostActions = angular.copy(allPostActions);

          let stages = fix(copyAllStages);
          let postActions = fix(copyPostActions);

          newConfig.config.stages = stages;
          newConfig.config.postActions = postActions;

          if (newConfig.__ui__) {
            delete newConfig.__ui__;
          }

          HydratorPlusPlusConfigStore.setState(HydratorPlusPlusConfigStore.getDefaults());
          $state.go('hydrator.create', { data: newConfig });
        };

        $scope.$on('$destroy', () => {
          sub();
        });

      },
      resolve: {
        rPipelineConfig: function () {
          return pipelineConfig;
        }
      }
    });

  }
}

angular.module(PKG.name + '.feature.hydrator')
  .service('HydratorUpgradeService', HydratorUpgradeService);
