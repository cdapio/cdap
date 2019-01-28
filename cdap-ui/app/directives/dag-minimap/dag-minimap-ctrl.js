/*
 * Copyright © 2019 Cask Data, Inc.
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

class DAGMinimapCtrl {
  constructor(DAGPlusPlusNodesStore, DAGMinimapUtilities, $scope, $window, $timeout) {
    this.DAGPlusPlusNodesStore = DAGPlusPlusNodesStore;
    this.DAGMinimapUtilities = DAGMinimapUtilities;
    this.$timeout = $timeout;

    this.state = {
      nodes: [],
      viewport: {}
    };

    DAGPlusPlusNodesStore.registerOnChangeListener(this.updateState.bind(this));

    $scope.$watch(() => this.canvasScale, this.updateState.bind(this));

    const debouncedUpdateContainerSize = _.debounce(this.updateDAGContainerSize.bind(this), 300);
    const windowElem = angular.element($window);
    windowElem.on('resize', debouncedUpdateContainerSize);

    $scope.$on('$destroy', () => {
      windowElem.off('resize', debouncedUpdateContainerSize);

      if (this.containerSizeTimeout) {
        this.$timeout.cancel(this.containerSizeTimeout);
      }
    });
  }

  updateDAGContainerSize() {
    this.containerSizeTimeout = this.$timeout(() => {
      const dagContainer = document.getElementById('diagram-container');
      this.dagContainerSize = dagContainer.getBoundingClientRect();
      this.updateState();
    });
  }

  getViewportBox() {
    if (!this.dagContainerSize) {
      this.updateDAGContainerSize();
      return;
    }

    const viewport = this.DAGMinimapUtilities.getViewportBox(
      this.graphMetadata,
      this.dagContainerSize,
      this.canvasScale,
      this.DAGPlusPlusNodesStore.getCanvasPanning()
    );

    if (!viewport) { return; }

    return {
      height: `${viewport.height}px`,
      width: `${viewport.width}px`,
      top: `${viewport.top}px`,
      left: `${viewport.left}px`,
    };
  }

  updateState() {
    const state = this.DAGPlusPlusNodesStore;
    const minimap = this.updateNodesAndViewport(state.getNodes());
    this.state.nodes = minimap.nodes;
    this.state.viewport = minimap.viewport;
  }

  updateNodesAndViewport(nodesReference) {
    const nodes = angular.copy(nodesReference);
    const graphMetadata = this.DAGMinimapUtilities.getGraphMetadata(nodes);

    this.graphMetadata = graphMetadata;

    nodes.forEach((node) => {
      const nodeMetadata = this.DAGMinimapUtilities.getNodeMetadata(node, graphMetadata);

      node.minimapStyle = {
        height: `${nodeMetadata.height}px`,
        width: `${nodeMetadata.width}px`,
        left: `${nodeMetadata.left}px`,
        top: `${nodeMetadata.top}px`
      };
    });

    const viewport = this.getViewportBox();

    return {
      nodes,
      viewport
    };
  }
}

angular.module(PKG.name + '.commons')
  .controller('DAGMinimapCtrl', DAGMinimapCtrl);
