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
  .controller('HydratorDetailCanvasController', function(rPipelineDetail, BottomPanelStore, NodesActionsFactory, HydratorService) {
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

    var obj = HydratorService.getNodesAndConnectionsFromConfig(rPipelineDetail);
    NodesActionsFactory.createGraphFromConfig(obj.nodes, obj.connections);

  });
