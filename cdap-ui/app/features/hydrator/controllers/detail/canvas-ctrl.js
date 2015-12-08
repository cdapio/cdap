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

angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorDetailCanvasController', function(rPipelineDetail, BottomPanelStore, NodesActionsFactory, HydratorService, NodesStore, ConfigStore, PipelineNodeConfigActionFactory, DetailNonRunsStore, MetricsStore) {
    this.ConfigStore = ConfigStore;
    this.NodesStore = NodesStore;
    this.DetailNonRunsStore = DetailNonRunsStore;
    this.PipelineNodeConfigActionFactory = PipelineNodeConfigActionFactory;
    this.HydratorService = HydratorService;
    this.MetricsStore = MetricsStore;

    try{
      rPipelineDetail.config = JSON.parse(rPipelineDetail.configuration);
    } catch(e) {
      console.log('ERROR in configuration from backend: ', e);
      return;
    }
    this.setState = function() {
      this.setScroll = (BottomPanelStore.getPanelState() === 0 ? false: true);
    };
    this.setState();
    BottomPanelStore.registerOnChangeListener(this.setState.bind(this));
    var obj = DetailNonRunsStore.getDAGConfig();
    NodesActionsFactory.createGraphFromConfig(obj.nodes, obj.connections);

    this.updateNodesAndConnections = function () {
      var activeNode = this.NodesStore.getActiveNodeId();
      if (!activeNode) {
        this.deleteNode();
      } else {
        this.setActiveNode();
      }
    };

    this.setActiveNode = function() {
      var nodeId = this.NodesStore.getActiveNodeId();
      if (!nodeId) {
        return;
      }
      this.PipelineNodeConfigActionFactory.choosePlugin(this.DetailNonRunsStore.getPluginObject(nodeId));
    };

    this.deleteNode = function() {
      this.PipelineNodeConfigActionFactory.removePlugin();
    };

    this.generateSchemaOnEdge = function (sourceId) {
      return this.HydratorService.generateSchemaOnEdge(sourceId);
    };

    function convertMetricsArrayIntoObject(arr, nodes) {
      var obj = {};

      angular.forEach(arr, function (item) {
        obj[nodes.name] = {
          recordsOut: item.recordsOut,
          recordsIn: item.recordsIn
        };
      });

      return obj;
    }
    var nodes = this.NodesStore.getNodes();
    this.metrics = convertMetricsArrayIntoObject(this.MetricsStore.getMetrics(), nodes);

    this.MetricsStore.registerOnChangeListener(function () {
      this.metrics = convertMetricsArrayIntoObject(this.MetricsStore.getMetrics(), nodes);
    }.bind(this));


    NodesStore.registerOnChangeListener(this.updateNodesAndConnections.bind(this));
  });
