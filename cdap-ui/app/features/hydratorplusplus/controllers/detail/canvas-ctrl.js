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
  .controller('HydratorPlusPlusDetailCanvasCtrl', function(rPipelineDetail, HydratorPlusPlusBottomPanelStore, DAGPlusPlusNodesActionsFactory, HydratorPlusPlusHydratorService, DAGPlusPlusNodesStore, ConfigStore, HydratorPlusPlusNodeConfigActions, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailMetricsStore) {
    this.ConfigStore = ConfigStore;
    this.DAGPlusPlusNodesStore = DAGPlusPlusNodesStore;
    this.HydratorPlusPlusDetailNonRunsStore = HydratorPlusPlusDetailNonRunsStore;
    this.HydratorPlusPlusNodeConfigActions = HydratorPlusPlusNodeConfigActions;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.HydratorPlusPlusDetailMetricsStore = HydratorPlusPlusDetailMetricsStore;

    try{
      rPipelineDetail.config = JSON.parse(rPipelineDetail.configuration);
    } catch(e) {
      console.log('ERROR in configuration from backend: ', e);
      return;
    }
    this.setState = function() {
      this.setScroll = (HydratorPlusPlusBottomPanelStore.getPanelState() === 0 ? false: true);
    };
    this.setState();
    HydratorPlusPlusBottomPanelStore.registerOnChangeListener(this.setState.bind(this));
    var obj = HydratorPlusPlusDetailNonRunsStore.getCloneConfig();

    DAGPlusPlusNodesActionsFactory.createGraphFromConfig(obj.__ui__.nodes, obj.config.connections, obj.config.comments);

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
      this.HydratorPlusPlusNodeConfigActions.choosePlugin(this.HydratorPlusPlusDetailNonRunsStore.getPluginObject(nodeId));
    };

    this.deleteNode = function() {
      this.HydratorPlusPlusNodeConfigActions.removePlugin();
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


    DAGPlusPlusNodesStore.registerOnChangeListener(this.updateNodesAndConnections.bind(this));
  });
