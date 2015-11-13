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

class HydratorCreateCanvasController {
  constructor(BottomPanelStore, NodesStore) {
    this.NodesStore = NodesStore;

    this.setState = () => {
      this.state = {
        setScroll: (BottomPanelStore.getPanelState() === 0? false: true)
      };
    };
    this.setState();
    BottomPanelStore.registerOnChangeListener(this.setState.bind(this));

    this.nodes = {};
    this.connections = [];

    this.updateNodesAndConnections();
    NodesStore.registerOnChangeListener(this.updateNodesAndConnections.bind(this));

  }

  updateNodesAndConnections() {
    this.nodes = this.NodesStore.getNodes();
    this.connections = this.NodesStore.getConnections();
  }
}


HydratorCreateCanvasController.$inject = ['BottomPanelStore', 'NodesStore'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorCreateCanvasController', HydratorCreateCanvasController);
  // .controller('HydratorCreateCanvasController', function(BottomPanelStore, NodesStore) {
  //   this.setState = function() {
  //     this.state = {
  //       setScroll: (BottomPanelStore.getPanelState() === 0? false: true)
  //     };
  //   };
  //   this.setState();
  //   BottomPanelStore.registerOnChangeListener(this.setState.bind(this));

  //   this.nodes = {};
  //   this.connections = [];

  //   function updateNodesAndConnections () {
  //     this.nodes = NodesStore.getNodes();
  //     this.connections = NodesStore.getConnections();

  //     console.log('nodes', this.nodes, this.connections);
  //   }

  //   updateNodesAndConnections();

  //   NodesStore.registerOnChangeListener(updateNodesAndConnections.bind(this));

  // });
