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

angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusDetailCanvasCtrl', function(rPipelineDetail, DAGPlusPlusNodesActionsFactory, HydratorPlusPlusHydratorService, DAGPlusPlusNodesStore, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailMetricsStore, $uibModal) {
    this.$uibModal = $uibModal;
    this.DAGPlusPlusNodesStore = DAGPlusPlusNodesStore;
    this.HydratorPlusPlusDetailNonRunsStore = HydratorPlusPlusDetailNonRunsStore;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.HydratorPlusPlusDetailMetricsStore = HydratorPlusPlusDetailMetricsStore;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;

    try{
      rPipelineDetail.config = JSON.parse(rPipelineDetail.configuration);
    } catch(e) {
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
            windowTemplateUrl: '/assets/features/hydratorplusplus/templates/partial/node-config-modal/popover-template.html',
            templateUrl: '/assets/features/hydratorplusplus/templates/partial/node-config-modal/popover.html',
            size: 'lg',
            backdrop: 'static',
            windowTopClass: 'node-config-modal hydrator-modal',
            controller: 'HydratorPlusPlusNodeConfigCtrl',
            controllerAs: 'HydratorPlusPlusNodeConfigCtrl',
            resolve: {
              rDisabled: function() {
                return true;
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
          recordsIn: item.recordsIn
        };
      });

      return obj;
    }
    this.metrics = convertMetricsArrayIntoObject(this.HydratorPlusPlusDetailMetricsStore.getMetrics());

    this.HydratorPlusPlusDetailMetricsStore.registerOnChangeListener(function () {
      this.metrics = convertMetricsArrayIntoObject(this.HydratorPlusPlusDetailMetricsStore.getMetrics());
    }.bind(this));


    DAGPlusPlusNodesStore.registerOnChangeListener(this.setActiveNode.bind(this));
  });
