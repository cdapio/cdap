/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

/*
  A small utility to bump the version of pipeline name while cloning the pipeline.

  Usage:
  getClonePipelineName('SamplePipeline') -> SamplePipeline_v1
  getClonePipelineName('SamplePipeline_v1') -> SamplePipeline_v2
  getClonePipelineName('SamplePipeline_v100') -> SamplePipeline_v101
  getClonePipelineName('SamplePipeline_v1_v3_v33') -> SamplePipeline_v1_v3_v34
  getClonePipelineName('SamplePipeline_22_v33') -> SamplePipeline_22_v34
  getClonePipelineName(1) -> 1_v1 (apparently this is allowed for a pipeline name).
*/

const getClonePipelineName = (name) => {
  name = typeof a === 'string' ? name : name.toString();
  let version = name.match(/(_v[\d]*)$/g);
  var existingSuffix; // For cases where pipeline name is of type 'SamplePipeline_v2_v4_v333'
  if (Array.isArray(version)) {
    version = version.pop();
    existingSuffix = version;
    version = version.replace('_v', '');
    version = '_v' + ((!isNaN(parseInt(version, 10)) ? parseInt(version, 10) : 1) + 1);
  } else {
    version = '_v1';
  }
  return name.split(existingSuffix)[0] + version;
};

class HydratorDetailTopPanelController {
  constructor(HydratorPlusPlusDetailRunsStore, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailActions, GLOBALS, $state, myLoadingService, $timeout, $scope, moment, myAlertOnValium, myPipelineExportModalService, myPipelineApi, myHelpers, myPreferenceApi, $q, $interval, MyPipelineStatusMapper, HydratorPlusPlusHydratorService, uuid) {
    this.GLOBALS = GLOBALS;
    this.myPipelineExportModalService = myPipelineExportModalService;
    this.myAlertOnValium = myAlertOnValium;
    this.$state = $state;
    this.moment = moment;
    this.$scope = $scope;
    this.$timeout = $timeout;
    this.scheduleTimeout = null;
    this.HydratorPlusPlusDetailNonRunsStore = HydratorPlusPlusDetailNonRunsStore;
    this.HydratorPlusPlusDetailRunsStore = HydratorPlusPlusDetailRunsStore;
    this.HydratorPlusPlusDetailActions = HydratorPlusPlusDetailActions;
    this.config = HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    this.cloneConfig = _.cloneDeep(HydratorPlusPlusDetailNonRunsStore.getCloneConfig());
    this.cloneConfig.name = getClonePipelineName(this.cloneConfig.name);
    this.myPipelineApi = myPipelineApi;
    this.myHelpers = myHelpers;
    this.myLoadingService = myLoadingService;
    this.myPreferenceApi = myPreferenceApi;
    this.$q = $q;
    this.uuid = uuid;
    this.$interval = $interval;
    this.MyPipelineStatusMapper = MyPipelineStatusMapper;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.app = {
      name: HydratorPlusPlusDetailNonRunsStore.getPipelineName(),
      description: this.config.description,
      type: this.config.artifact.name
    };
    this.viewSettings = false;
    this.viewLogs = false;
    this.viewScheduler = false;
    this.viewConfig = false;
    this.pipelineType = HydratorPlusPlusDetailNonRunsStore.getPipelineType();
    this.tooltipDescription = (this.app.description && this.app.description.replace(/\n/g, '<br />')) || '' ;
    this.setState();
    this.setAppStatus();
    this.pipelineDurationTimer = null;
    var appType = HydratorPlusPlusDetailNonRunsStore.getAppType();
    if (GLOBALS.etlBatchPipelines.indexOf(appType) !== -1) {
      HydratorPlusPlusDetailActions.fetchScheduleStatus(
        HydratorPlusPlusDetailRunsStore.getApi(),
        HydratorPlusPlusDetailRunsStore.getScheduleParams()
      );
    }
    HydratorPlusPlusDetailRunsStore.registerOnChangeListener(() => {
      this.setAppStatus();
      this.setState();
    });
    HydratorPlusPlusDetailNonRunsStore.registerOnChangeListener(this.setScheduleStatus.bind(this));

    this.macrosMap = HydratorPlusPlusDetailRunsStore.getMacros();
    this.userRuntimeArgumentsMap = HydratorPlusPlusDetailRunsStore.getUserRuntimeArguments();
    this.runtimeArguments = {};
    this.resolvedMacros = {};
    this.validToStartOrSchedule = true;
    this.pipelineAction = 'Run';

    this.isCloneDisabled = [GLOBALS.etlBatch, GLOBALS.etlRealtime].indexOf(HydratorPlusPlusDetailNonRunsStore.getArtifact().name) !== -1;

    if (Object.keys(this.macrosMap).length === 0) {
      this.fetchMacros();
    }

    this.viewInCdapLink = window.getAbsUIUrl({
      namespaceId: $state.params.namespace,
      entityType: 'apps',
      entityId: this.app.name
    });

    this.$scope.$on('$destroy', () => {
      this.$timeout.cancel(this.scheduleTimeout);
      this.$interval.cancel(this.pipelineDurationTimer);
    });
  }

  fetchMacros() {
    const parseMacros = macrosSpec => {
      let macrosObj = {};
      for(let i = 0; i < macrosSpec.length; i++){
        if(this.myHelpers.objectQuery(macrosSpec[i], 'spec', 'properties', 'macros', 'lookupProperties') &&
          this.myHelpers.objectQuery(macrosSpec[i], 'spec', 'properties', 'macros', 'lookupProperties').length > 0){
            let macrosKeys = this.myHelpers.objectQuery(macrosSpec[i], 'spec', 'properties', 'macros', 'lookupProperties');

            macrosKeys.forEach((key) => {
              macrosObj[key] = '';
            });
        }
      }
      return macrosObj;
    };

    let {namespace, app} = this.HydratorPlusPlusDetailRunsStore.getParams();

    return this.myPipelineApi
      .fetchMacros({
        namespace,
        pipeline: app
      })
      .$promise
      .then(
        (res) => {
          this.macrosMap = parseMacros(res);
          return this.macrosMap;
        },
        err => this.macroError = err
      )
      .then(
        () => {
          this.HydratorPlusPlusDetailActions.setMacros(this.macrosMap);
          this.getRuntimeArguments();
        }
      );
  }

  getRuntimeArguments() {
    this.macrosMap = this.HydratorPlusPlusDetailRunsStore.getMacros();
    this.userRuntimeArgumentsMap = this.HydratorPlusPlusDetailRunsStore.getUserRuntimeArguments();

    // if there are no runtime arguments at all
    if (Object.keys(this.macrosMap).length === 0 && Object.keys(this.userRuntimeArgumentsMap).length === 0) {
      this.runtimeArguments.pairs = [{
        key: '',
        value: '',
        uniqueId: 'id-' + this.uuid.v4()
      }];
      return this.$q.when(this.runtimeArguments);
    }

    // if there are non-zero number of macros
    if (Object.keys(this.macrosMap).length !== 0) {
      let preferenceParams = {
        namespace: this.$state.params.namespace,
        appId: this.$state.params.pipelineId,
        scope: this.$scope
      };

      return this.myPreferenceApi
        .getAppPreferenceResolved(preferenceParams)
        .$promise
        .then(
          (res) => {
            let newResolvedMacros = this.HydratorPlusPlusHydratorService.getPrefsRelevantToMacros(res, this.macrosMap);

            // if the higher level preferences have changed, or if this.resolvedMacros is empty
            if (!angular.equals(newResolvedMacros, this.resolvedMacros)) {
              if (Object.keys(this.resolvedMacros).length === 0) {
                this.macrosMap = Object.assign(this.macrosMap, newResolvedMacros);
              } else {
                let newPrefs = {};
                for (let macroKey in newResolvedMacros) {
                  if (newResolvedMacros.hasOwnProperty(macroKey) &&
                    this.resolvedMacros.hasOwnProperty(macroKey) &&
                    this.macrosMap.hasOwnProperty(macroKey)) {
                    if (newResolvedMacros[macroKey] !== this.resolvedMacros[macroKey] &&
                        this.resolvedMacros[macroKey] === this.macrosMap[macroKey]) {
                      newPrefs[macroKey] = newResolvedMacros[macroKey];
                    }
                  }
                }
                // only update the macros that have new resolved values
                if (Object.keys(newPrefs).length > 0) {
                  this.macrosMap = Object.assign({}, this.macrosMap, newPrefs);
                }
              }

              this.resolvedMacros = newResolvedMacros;
              this.HydratorPlusPlusDetailActions.setMacros(this.macrosMap);
            }

            if (Object.keys(this.macrosMap).length > 0 || Object.keys(this.userRuntimeArgumentsMap).length > 0) {
              this.runtimeArguments = this.HydratorPlusPlusHydratorService.convertMacrosToRuntimeArguments(this.runtimeArguments, this.macrosMap, this.userRuntimeArgumentsMap);
            }
            this.validToStartOrSchedule = this.isValidToStartOrSchedule();
            return this.runtimeArguments;
          },
          err => this.macroError = err
        );

    // if there are zero macros, but there are user-set runtime arguments
    } else {
      this.runtimeArguments = this.HydratorPlusPlusHydratorService.convertMacrosToRuntimeArguments(this.runtimeArguments, this.macrosMap, this.userRuntimeArgumentsMap);
      this.validToStartOrSchedule = this.isValidToStartOrSchedule();
      return this.$q.when(this.runtimeArguments);
    }
  }

  applyRuntimeArguments() {
    let macros = this.HydratorPlusPlusHydratorService.convertRuntimeArgsToMacros(this.runtimeArguments);
    this.macrosMap = macros.macrosMap;
    this.userRuntimeArgumentsMap = macros.userRuntimeArgumentsMap;
    // have to do this because cannot do two dispatch in a row
    this.HydratorPlusPlusDetailActions.setMacrosAndUserRuntimeArguments(this.macrosMap, this.userRuntimeArgumentsMap);
    this.validToStartOrSchedule = this.isValidToStartOrSchedule();
  }

  isValidToStartOrSchedule() {
    return !this.HydratorPlusPlusHydratorService.keyValuePairsHaveMissingValues(this.runtimeArguments);
  }

  setState() {
    var latestRun = this.HydratorPlusPlusDetailRunsStore.getLatestRun();
    var lastRunDuration;

    if (latestRun) {
      this.lastFinished = latestRun;
    }

    if (this.lastFinished) {

      if (this.lastFinished.status === 'RUNNING') {

        if (!this.pipelineDurationTimer) {
          this.pipelineDurationTimer = this.$interval(() => {

            if (this.lastFinished.end) {
              let endDuration = this.lastFinished.end - this.lastFinished.start;
              this.lastRunTime = typeof this.lastFinished.end === 'number' ? this.moment.utc(endDuration * 1000).format('HH:mm:ss') : 'N/A';
            } else {
              let runningDuration = new Date().getTime() - (this.lastFinished.start * 1000);
              this.lastRunTime = this.moment.utc(runningDuration).format('HH:mm:ss');
            }
          }, 1000);
        }
      }

      lastRunDuration = this.lastFinished.end - this.lastFinished.start;

      let setInitialTimer = new Date().getTime() - (this.lastFinished.start * 1000);
      this.lastRunTime = typeof this.lastFinished.end === 'number' ?
                          this.moment.utc(lastRunDuration * 1000).format('HH:mm:ss') :
                          this.moment.utc(setInitialTimer).format('HH:mm:ss');
    } else {
      this.lastRunTime = 'N/A';
    }
    this.config = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    this.macrosMap = this.HydratorPlusPlusDetailRunsStore.getMacros();
    this.userRuntimeArgumentsMap = this.HydratorPlusPlusDetailRunsStore.getUserRuntimeArguments();
  }

  setAppStatus() {
    this.appStatus = this.MyPipelineStatusMapper.lookupDisplayStatus(
      this.HydratorPlusPlusDetailRunsStore.getStatus());

    if (this.appStatus === 'Succeeded' || this.appStatus === 'Stopped') {
      this.$interval.cancel(this.pipelineDurationTimer);
      this.pipelineDurationTimer = null;
    }

    this.config = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
  }
  setScheduleStatus() {
    this.scheduleStatus = this.MyPipelineStatusMapper.lookupDisplayStatus(this.HydratorPlusPlusDetailNonRunsStore.getScheduleStatus());
  }
  isGreenStatus() {
    var greenStatus = ['Succeeded', 'Starting', 'Scheduling', 'Stopping'];
    if (greenStatus.indexOf(this.appStatus) > -1) {
      return true;
    } else {
      return false;
    }
  }
  exportConfig() {
    let exportConfig = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    delete exportConfig.__ui__;
    this.myPipelineExportModalService.show(exportConfig, exportConfig);
  }
  do(action) {
    switch(action) {
      case 'Run':
        this.getRuntimeArguments()
          .then(() => {
            this.startPipeline();
          }, (err) => {console.log('Error: ', err); });
        break;
      case 'Schedule':
        this.getRuntimeArguments()
          .then(() => {
            this.viewScheduler = true;
          });
        break;
      case 'Config':
        this.getRuntimeArguments()
          .then(() => {
            this.viewConfig = true;
          });
        break;
      case 'Suspend':
        this.viewScheduler = true;
        break;
      case 'Stop':
        this.appStatus = this.MyPipelineStatusMapper.lookupDisplayStatus('STOPPING');
        this.stopPipeline();
        break;
      case 'Delete':
        this.myLoadingService.showLoadingIcon();
        var params = this.HydratorPlusPlusDetailRunsStore.getParams();
        params = {
          namespace: params.namespace,
          pipeline: params.app
        };
        this.HydratorPlusPlusDetailActions
          .deletePipeline(params)
          .then(
            () => {
              this.$state.go('hydrator.list');
              this.myLoadingService.hideLoadingIcon();
            },
            (err) => {
              this.myLoadingService.hideLoadingIcon();
              this.$timeout(() => {
                this.myAlertOnValium.show({
                  type: 'danger',
                  title: 'Unable to delete Pipeline:',
                  content: err.data
                });
              });
            }
          );
    }
  }
  doStartScheduleOrConfig(action) {
    if (this.validToStartOrSchedule) {
      this.do(action);
    } else {
      this.pipelineAction = action;
      this.do('Config');
    }
  }
  startOrSchedulePipeline() {
    if (this.pipelineAction === 'Run') {
      this.startPipeline();
    } else if (this.pipelineAction === 'Schedule') {
      this.viewConfig = false;
      this.pipelineAction = 'Run';
      this.viewScheduler = true;
    }
  }
  startPipeline() {
    this.lastRunTime = 'N/A';
    this.lastFinished = null;
    this.viewConfig = false;
    this.appStatus = this.MyPipelineStatusMapper.lookupDisplayStatus('STARTING');
    let macrosWithNonEmptyValues = this.HydratorPlusPlusHydratorService.getMacrosWithNonEmptyValues(this.macrosMap);
    this.HydratorPlusPlusDetailActions.startPipeline(
      this.HydratorPlusPlusDetailRunsStore.getApi(),
      this.HydratorPlusPlusDetailRunsStore.getParams(),
      Object.assign({}, macrosWithNonEmptyValues, this.userRuntimeArgumentsMap)
    )
    .then(
      () => {},
      (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          title: 'Unable to start a new run:',
          content: angular.isObject(err) ? err.data: err
        });
      }
    );
  }
  stopPipeline() {
    this.HydratorPlusPlusDetailActions.stopPipeline(
      this.HydratorPlusPlusDetailRunsStore.getApi(),
      this.HydratorPlusPlusDetailRunsStore.getParams()
    ).then(
      () => {},
      (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          title: 'Unable to stop the current run,',
          content: angular.isObject(err) ? err.data: err
        });
      }
    );
  }

  schedulePipeline() {
    this.scheduleStatus = this.MyPipelineStatusMapper.lookupDisplayStatus('SCHEDULING');
    this.scheduleLoading = true;
    let preferenceParams = {
      namespace: this.$state.params.namespace,
      appId: this.$state.params.pipelineId,
      scope: this.$scope
    };

    let macrosWithNonEmptyValues = this.HydratorPlusPlusHydratorService.getMacrosWithNonEmptyValues(this.macrosMap);

    this.myPreferenceApi
      .setAppPreference(preferenceParams, Object.assign({}, macrosWithNonEmptyValues, this.userRuntimeArgumentsMap))
      .$promise
      .then(
        () => {
          return this.HydratorPlusPlusDetailActions.schedulePipeline(
            this.HydratorPlusPlusDetailRunsStore.getApi(),
            this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
          )
            .then(() => {
              if (this.scheduleTimeout) {
                this.$timeout.cancel(this.scheduleTimeout);
              }
              this.scheduleTimeout = this.$timeout(() => {
                this.scheduleLoading = false;
              }, 1000);
            });
        },
        (err) => {
          this.scheduleLoading = false;
          this.macroError = angular.isObject(err) ? (err.data || 'Error saving runtime arguments in preferences store') : err;
          this.setScheduleStatus();
          return this.$q.reject();
        }
      )
      .then(
        () => {
          this.HydratorPlusPlusDetailActions.fetchScheduleStatus(
            this.HydratorPlusPlusDetailRunsStore.getApi(),
            this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
          );
        },
        (err) => {
          if (!this.macroError) {
            this.myAlertOnValium.show({
              type: 'danger',
              title: 'Unable to schedule the pipeline:',
              content: angular.isObject(err) ? err.data : err
            });
          }
          this.setScheduleStatus();
          return this.$q.reject();
        }
      )
      .then(
        () => {
          this.viewScheduler = false;
        }
      );
  }
  suspendPipeline() {
    this.scheduleLoading = true;
    this.HydratorPlusPlusDetailActions.suspendSchedule(
      this.HydratorPlusPlusDetailRunsStore.getApi(),
      this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
    )
      .then(
        () => {
          this.HydratorPlusPlusDetailActions.fetchScheduleStatus(
            this.HydratorPlusPlusDetailRunsStore.getApi(),
            this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
          );

          if (this.scheduleTimeout) {
            this.$timeout.cancel(this.scheduleTimeout);
          }
          this.scheduleTimeout = this.$timeout(() => {
            this.scheduleLoading = false;
          }, 1000);
        },
        (err) => {
          this.myAlertOnValium.show({
            type: 'danger',
            title: 'Unable to suspend the pipeline::',
            content: angular.isObject(err) ? err.data: err
          });
          this.scheduleLoading = false;
        }
      );
  }
}

HydratorDetailTopPanelController.$inject = ['HydratorPlusPlusDetailRunsStore', 'HydratorPlusPlusDetailNonRunsStore', 'HydratorPlusPlusDetailActions', 'GLOBALS', '$state', 'myLoadingService', '$timeout', '$scope', 'moment', 'myAlertOnValium', 'myPipelineExportModalService', 'myPipelineApi', 'myHelpers', 'myPreferenceApi', '$q', '$interval', 'MyPipelineStatusMapper', 'HydratorPlusPlusHydratorService', 'uuid'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorDetailTopPanelController', HydratorDetailTopPanelController);
