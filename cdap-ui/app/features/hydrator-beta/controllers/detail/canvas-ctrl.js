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

angular.module(PKG.name + '.feature.hydrator-beta')
  .controller('HydratorDetailCanvasControllerBeta', function(rPipelineDetail, BottomPanelStoreBeta, NodesActionsFactory, HydratorServiceBeta, NodesStore, ConfigStoreBeta, PipelineNodeConfigActionFactoryBeta, DetailNonRunsStore, MetricsStore) {
    this.ConfigStoreBeta = ConfigStoreBeta;
    this.NodesStore = NodesStore;
    this.DetailNonRunsStore = DetailNonRunsStore;
    this.PipelineNodeConfigActionFactoryBeta = PipelineNodeConfigActionFactoryBeta;
    this.HydratorServiceBeta = HydratorServiceBeta;
    this.MetricsStore = MetricsStore;

    try{
      rPipelineDetail.config = JSON.parse(rPipelineDetail.configuration);
    } catch(e) {
      console.log('ERROR in configuration from backend: ', e);
      return;
    }
    this.setState = function() {
      this.setScroll = (BottomPanelStoreBeta.getPanelState() === 0 ? false: true);
    };
    this.setState();
    BottomPanelStoreBeta.registerOnChangeListener(this.setState.bind(this));
    var obj = DetailNonRunsStore.getCloneConfig();

    NodesActionsFactory.createGraphFromConfig(obj.__ui__.nodes, obj.config.connections, obj.config.comments);

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
      this.PipelineNodeConfigActionFactoryBeta.choosePlugin(this.DetailNonRunsStore.getPluginObject(nodeId));
    };

    this.deleteNode = function() {
      this.PipelineNodeConfigActionFactoryBeta.removePlugin();
    };

    this.generateSchemaOnEdge = function (sourceId) {
      return this.HydratorServiceBeta.generateSchemaOnEdge(sourceId);
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
    this.metrics = convertMetricsArrayIntoObject(this.MetricsStore.getMetrics());

    this.MetricsStore.registerOnChangeListener(function () {
      this.metrics = convertMetricsArrayIntoObject(this.MetricsStore.getMetrics());
    }.bind(this));


    NodesStore.registerOnChangeListener(this.updateNodesAndConnections.bind(this));
  });
