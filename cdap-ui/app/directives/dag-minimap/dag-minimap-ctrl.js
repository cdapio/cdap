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
  constructor(DAGPlusPlusNodesStore, DAGMinimapUtilities, $scope, $window, $timeout, DAGPlusPlusNodesActionsFactory) {
    this.DAGPlusPlusNodesStore = DAGPlusPlusNodesStore;
    this.DAGMinimapUtilities = DAGMinimapUtilities;
    this.$timeout = $timeout;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.MINIMAP_MIN_NUMBER_NODES = 6;

    this.state = {
      nodes: [],
      viewport: {}
    };

    DAGPlusPlusNodesStore.registerOnChangeListener(this.updateState.bind(this));

    $scope.$watch(() => this.canvasScale, this.updateState.bind(this));

    const debouncedUpdateContainerSize = _.debounce(this.updateDAGContainerSize.bind(this), 300);
    const windowElem = angular.element($window);
    windowElem.on('resize', debouncedUpdateContainerSize);

    this.viewportTimeout = $timeout(this.handleViewportDrag.bind(this));

    $scope.$on('$destroy', () => {
      windowElem.off('resize', debouncedUpdateContainerSize);

      if (this.containerSizeTimeout) {
        this.$timeout.cancel(this.containerSizeTimeout);
      }
      if (this.viewportTimeout) {
        this.$timeout.cancel(this.viewportTimeout);
      }
      if (this.updateStateTimeout) {
        this.$timeout.cancel(this.updateStateTimeout);
      }
    });
  }

  handleViewportDrag() {
    this.viewportElem = document.getElementById('viewport-container');

    this.viewportElem.addEventListener('mousedown', this.dragStart.bind(this));
    this.viewportElem.addEventListener('mousemove', this.drag.bind(this));
    this.viewportElem.addEventListener('mouseup', this.dragEnd.bind(this));
  }

  dragStart(e) {
    if (e.which !== 1) { return; } // should only apply for left mouse click

    this.viewportRect = this.viewportElem.getBoundingClientRect();
    this.active = true;
    this.viewportDragHandler(e);
  }

  dragEnd() {
    this.active = false;
  }

  drag(e) {
    if (!this.active) { return; }
    e.preventDefault();
    this.viewportDragHandler(e);
  }

  viewportDragHandler(e) {
    const posX = e.clientX - this.viewportRect.x;
    const posY = e.clientY - this.viewportRect.y;

    // if drag goes out of the boundary of the minimap
    if (
      posX <= 0 ||
      posY <= 0 ||
      posX >= this.viewportRect.width ||
      posY >= this.viewportRect.height
    ) {
      this.dragEnd();
      return;
    }

    const viewportPosition = this.DAGMinimapUtilities.getViewportLocation(
      posX,
      posY,
      this.graphMetadata,
      this.canvasScale
    );

    this.DAGPlusPlusNodesActionsFactory.setCanvasPanning({
      top: viewportPosition.y,
      left: viewportPosition.x,
    });

    this.panning({
      top: viewportPosition.y,
      left: viewportPosition.x,
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
      return {};
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

    if (this.updateStateTimeout) {
      this.$timeout.cancel(this.updateStateTimeout);
    }
    this.updateStateTimeout = this.$timeout(() => {
      this.state = {
        nodes: minimap.nodes,
        viewport: minimap.viewport,
      };
    });
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
