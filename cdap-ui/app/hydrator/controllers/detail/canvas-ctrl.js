/*
 * Copyright © 2016 Cask Data, Inc.
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
  .controller('HydratorPlusPlusDetailCanvasCtrl', function(rPipelineDetail, DAGPlusPlusNodesActionsFactory, HydratorPlusPlusHydratorService, DAGPlusPlusNodesStore, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailMetricsStore, $uibModal, HydratorPlusPlusDetailRunsStore, MyPipelineStatusMapper, moment, $interval) {
    this.$uibModal = $uibModal;
    this.DAGPlusPlusNodesStore = DAGPlusPlusNodesStore;
    this.HydratorPlusPlusDetailNonRunsStore = HydratorPlusPlusDetailNonRunsStore;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.HydratorPlusPlusDetailMetricsStore = HydratorPlusPlusDetailMetricsStore;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.MyPipelineStatusMapper = MyPipelineStatusMapper;
    this.$interval = $interval;
    this.moment = moment;
    this.currentRunTimeCounter = null;
    try {
      rPipelineDetail.config = JSON.parse(rPipelineDetail.configuration);
    } catch (e) {
      console.log('ERROR in configuration from backend: ', e);
      return;
    }
    var obj = HydratorPlusPlusDetailNonRunsStore.getCloneConfig();

    this.DAGPlusPlusNodesActionsFactory.createGraphFromConfig(obj.__ui__.nodes, obj.config.connections, obj.config.comments);

    this.updateNodesAndConnections = function () {
      var activeNode = this.DAGPlusPlusNodesStore.getActiveNodeId();
      if (!activeNode) {
        this.deleteNode();
      } else {
        this.setActiveNode();
      }
    };


    this.setActiveNode = function() {
      var nodeId = this.DAGPlusPlusNodesStore.getActiveNodeId();
      if (!nodeId) {
        return;
      }
      let pluginNode = this.HydratorPlusPlusDetailNonRunsStore.getPluginObject(nodeId);
      this.$uibModal
          .open({
            windowTemplateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/popover-template.html',
            templateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/popover.html',
            size: 'lg',
            backdrop: 'static',
            windowTopClass: 'node-config-modal hydrator-modal',
            controller: 'HydratorPlusPlusNodeConfigCtrl',
            controllerAs: 'HydratorPlusPlusNodeConfigCtrl',
            resolve: {
              rIsStudioMode: function () {
                return false;
              },
              rDisabled: function() {
                return true;
              },
              rNodeMetricsContext: function() {
                var appParams = HydratorPlusPlusDetailRunsStore.getParams();
                var latestRun = HydratorPlusPlusDetailRunsStore.getLatestRun();
                var logsParams = HydratorPlusPlusDetailRunsStore.getLogsParams();
                let runs = HydratorPlusPlusDetailRunsStore.getRuns();
                return {
                  runRecord: latestRun,
                  runs,
                  namespace: appParams.namespace,
                  app: appParams.app,
                  programType: HydratorPlusPlusDetailRunsStore.getMetricProgramType(),
                  programId: logsParams.programId
                };
              },
              rPlugin: ['HydratorPlusPlusNodeService', 'HydratorPlusPlusDetailNonRunsStore', 'GLOBALS', function(HydratorPlusPlusNodeService, HydratorPlusPlusDetailNonRunsStore, GLOBALS) {
                let pluginId = pluginNode.name;
                let appType = HydratorPlusPlusDetailNonRunsStore.getAppType();
                let artifactVersion = HydratorPlusPlusDetailNonRunsStore.getArtifact().version;
                let sourceConn = HydratorPlusPlusDetailNonRunsStore
                  .getSourceNodes(pluginId)
                  .filter( node => typeof node.outputSchema === 'string');
                return HydratorPlusPlusNodeService
                  .getPluginInfo(pluginNode, appType, sourceConn, artifactVersion)
                  .then((nodeWithInfo) => (
                    {
                      node: nodeWithInfo,
                      isValidPlugin: true,
                      type: appType,
                      isSource: GLOBALS.pluginConvert[nodeWithInfo.type] === 'source',
                      isSink: GLOBALS.pluginConvert[nodeWithInfo.type] === 'sink',
                      isTransform: GLOBALS.pluginConvert[nodeWithInfo.type] === 'transform',
                      isAction: GLOBALS.pluginConvert[nodeWithInfo.type] === 'action'
                    }
                  ));
              }]
            }
          })
          .result
          .then(this.deleteNode.bind(this), this.deleteNode.bind(this)); // Both close and ESC events in the modal are considered as SUCCESS and ERROR in promise callback. Hence the same callback for both success & failure.
    };

    this.deleteNode = () => {
      this.DAGPlusPlusNodesActionsFactory.resetSelectedNode();
    };

    this.generateSchemaOnEdge = function (sourceId) {
      return this.HydratorPlusPlusHydratorService.generateSchemaOnEdge(sourceId);
    };

    function convertMetricsArrayIntoObject(arr) {
      var obj = {};

      angular.forEach(arr, function (item) {
        obj[item.nodeName] = {
          recordsOut: item.recordsOut,
          recordsIn: item.recordsIn,
          recordsError: item.recordsError
        };
      });

      return obj;
    }
    this.metrics = convertMetricsArrayIntoObject(this.HydratorPlusPlusDetailMetricsStore.getMetrics());
    this.logsMetrics = this.HydratorPlusPlusDetailMetricsStore.getLogsMetrics();

    this.HydratorPlusPlusDetailMetricsStore.registerOnChangeListener(function () {
      this.metrics = convertMetricsArrayIntoObject(this.HydratorPlusPlusDetailMetricsStore.getMetrics());
      this.logsMetrics = this.HydratorPlusPlusDetailMetricsStore.getLogsMetrics();
    }.bind(this));

    HydratorPlusPlusDetailRunsStore.registerOnChangeListener(() => {
      let runs = HydratorPlusPlusDetailRunsStore.getRuns().reverse();
      this.currentRun = HydratorPlusPlusDetailRunsStore.getLatestRun();
      let status = this.MyPipelineStatusMapper.lookupDisplayStatus(this.currentRun.status);
      this.$interval.cancel(this.currentRunTimeCounter);
      if (status === 'Running') {
        this.currentRunTimeCounter = this.$interval(() => {
          let duration = window.CaskCommon.CDAPHelpers.humanReadableDuration(Math.floor(Date.now() / 1000) - this.currentRun.start);
          this.currentRun = Object.assign({}, this.currentRun, {
            duration
          });
        }, 1000);
      }
      let timeDifference = this.currentRun.end ? this.currentRun.end - this.currentRun.start : Math.floor(Date.now() / 1000) - this.currentRun.start;
      this.currentRun = Object.assign({}, this.currentRun, {
        duration: window.CaskCommon.CDAPHelpers.humanReadableDuration(timeDifference),
        startTime: this.currentRun.start ? this.moment(this.currentRun.start * 1000).format('hh:mm:ss a') : null,
        starting: !this.currentRun.start ? this.currentRun.starting : null,
        statusCssClass: this.MyPipelineStatusMapper.getStatusIndicatorClass(status),
        status
      });
      let runNumber = _.findIndex(runs, {runid: this.currentRun.runid});
      this.currentRunIndex = runNumber + 1;
      this.totalRuns = runs.length;
    });

    DAGPlusPlusNodesStore.registerOnChangeListener(this.setActiveNode.bind(this));
  });
