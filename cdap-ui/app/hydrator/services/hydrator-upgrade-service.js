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
  constructor($rootScope, myPipelineApi, $state, $uibModal, HydratorPlusPlusConfigStore) {
    this.$rootScope = $rootScope;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;
    this.$uibModal = $uibModal;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
  }

  _checkVersionIsInRange(range, version) {
    if (!range || !version) { return false; }

    if (['[', '('].indexOf(range[0]) !== -1) {
      const supportedVersion = new window.CaskCommon.Version(version);
      const versionRange = new window.CaskCommon.VersionRange(range);

      if (versionRange.versionIsInRange(supportedVersion)) {
        return true;
      } else {
        return false;
      }
    }

    // Check equality if range is just a single version
    if (range !== version) {
      return false;
    }

    return true;
  }

  checkPipelineArtifactVersion(config) {
    if (!config || !config.artifact) { return false; }

    let cdapVersion = this.$rootScope.cdapVersion;

    return this._checkVersionIsInRange(config.artifact.version, cdapVersion);
  }

  /**
   * Create artifacts map based on the artifacts array we get from backend.
   * This will be a map of artifact name with the latest artifact version info.
   * If there exist 2 artifacts with same version, it will maintain both scopes in an array.
   **/
  _createArtifactsMap(artifactsList) {
    let artifactsMap = {};

    artifactsList.forEach((artifact) => {
      let name = artifact.name;

      if (!artifactsMap[name]) {
        artifactsMap[name] = artifact;
      } else if (artifactsMap[name].version === artifact.version) {
        artifactsMap[name].scope = [artifactsMap[name].scope, artifact.scope];
      } else {
        let prevVersion = new window.CaskCommon.Version(artifactsMap[name].version);
        let currVersion = new window.CaskCommon.Version(artifact.version);

        if (currVersion.compareTo(prevVersion) === 1) {
          artifactsMap[name] = artifact;
        }
      }
    });

    return artifactsMap;
  }

  getNonExistingStages(pipelineConfig) {
    let stages = pipelineConfig.config.stages;

    return this.myPipelineApi.getAllArtifacts({namespace: this.$state.params.namespace})
      .$promise
      .then((res) => {
        let artifactsMap = this._createArtifactsMap(res);

        let transformedStages = [];

        stages.forEach((stage) => {
          let stageArtifact = stage.plugin.artifact;

          let data = {
            stageInfo: stage,
            error: null
          };

          if (!artifactsMap[stageArtifact.name]) {
            data.error = 'NOTFOUND';
          } else if (!this._checkVersionIsInRange(stageArtifact.version, artifactsMap[stageArtifact.name].version)) {
            data.error = 'VERSION_MISMATCH';
            data.suggestion = artifactsMap[stageArtifact.name];

            if (typeof data.suggestion.scope !== 'string') {
              // defaulting to USER scope when both version exists
              data.suggestion.scope = 'USER';
            }
          } else if (artifactsMap[stageArtifact.name].scope.indexOf(stageArtifact.scope) < 0) {
            data.error = 'SCOPE_MISMATCH';
            data.suggestion = artifactsMap[stageArtifact.name];
          }

          transformedStages.push(data);
        });

        return transformedStages;

      }, (err) => {
        console.log('cannot fetch artifacts', err);
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
      windowTopClass: 'hydrator-modal center upgrade-modal',
      controllerAs: 'PipelineUpgradeController',
      controller: function ($scope, rPipelineConfig, HydratorUpgradeService, $rootScope, HydratorPlusPlusConfigStore, $state, DAGPlusPlusFactory, GLOBALS) {
        let eventEmitter = window.CaskCommon.ee(window.CaskCommon.ee);
        let globalEvents = window.CaskCommon.globalEvents;


        this.pipelineConfig = rPipelineConfig;
        this.cdapVersion = $rootScope.cdapVersion;

        this.pipelineArtifact = HydratorUpgradeService.checkPipelineArtifactVersion(rPipelineConfig);

        this.problematicStages = [];
        this.missingArtifactStages = [];
        let allStages = [];
        this.fixAllDisabled = true;

        const checkStages = () => {
          HydratorUpgradeService.getNonExistingStages(rPipelineConfig)
            .then((res) => {
              allStages = res.map((stage) => {
                stage.icon = DAGPlusPlusFactory.getIcon(stage.stageInfo.plugin.name.toLowerCase());
                stage.type = GLOBALS.pluginConvert[stage.stageInfo.plugin.type];
                return stage;
              });

              this.problematicStages = [];
              this.missingArtifactStages = [];

              res.forEach((artifact) => {
                if (artifact.error === 'NOTFOUND') {
                  this.missingArtifactStages.push(artifact);
                } else if (artifact.error) {
                  this.problematicStages.push(artifact);
                }
              });

              this.fixAllDisabled = this.missingArtifactStages.length > 0;
            });

          eventEmitter.off(globalEvents.MARKETCLOSING, checkStages);
        };

        checkStages();

        this.openMarket = () => {
          eventEmitter.emit(globalEvents.OPENMARKET);
          eventEmitter.on(globalEvents.MARKETCLOSING, checkStages);
        };

        this.fixAll = () => {
          let newConfig = HydratorUpgradeService.upgradePipelineArtifactVersion(rPipelineConfig);

          let stages = allStages.map((stage) => {
            let updatedStageInfo = stage.stageInfo;

            if (stage.error) {
              updatedStageInfo.plugin.artifact = stage.suggestion;
            }

            return updatedStageInfo;
          });

          newConfig.config.stages = stages;

          HydratorPlusPlusConfigStore.setState(HydratorPlusPlusConfigStore.getDefaults());
          $state.go('hydrator.create', { data: newConfig });
        };


        $scope.$on('$destroy', () => {
          eventEmitter.off(globalEvents.MARKETCLOSING, checkStages);
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
