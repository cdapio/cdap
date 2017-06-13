/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

class DAGPlusPlusNodesStore {
  constructor(DAGPlusPlusNodesDispatcher, uuid, GLOBALS) {
    this.state = {};
    this.setDefaults();
    this.changeListeners = [];
    this.uuid = uuid;
    this.GLOBALS = GLOBALS;

    let dispatcher = DAGPlusPlusNodesDispatcher.getDispatcher();
    dispatcher.register('onNodeAdd', this.addNode.bind(this));
    dispatcher.register('onRemoveNode', this.removeNode.bind(this));
    dispatcher.register('onConnect', this.addConnection.bind(this));
    dispatcher.register('onConnectionsUpdate', this.updateConnections.bind(this));
    dispatcher.register('onRemoveConnection', this.removeConnection.bind(this));
    dispatcher.register('onReset', this.setDefaults.bind(this));
    dispatcher.register('onNodeSelect', this.setActiveNodeId.bind(this));
    dispatcher.register('onCreateGraphFromConfig', this.setNodesAndConnections.bind(this));
    dispatcher.register('onNodeSelectReset', this.resetActiveNode.bind(this));
    dispatcher.register('onNodeUpdate', this.updateNode.bind(this));
    dispatcher.register('onResetPluginCount', this.resetPluginCount.bind(this));
    dispatcher.register('onSetCanvasPanning', this.setCanvasPanning.bind(this));
    dispatcher.register('onAddComment', this.addComment.bind(this));
    dispatcher.register('onSetComments', this.setComments.bind(this));
    dispatcher.register('onDeleteComment', this.deleteComment.bind(this));
    dispatcher.register('onUpdateComment', this.updateComment.bind(this));
    dispatcher.register('onUndoActions', this.undoActions.bind(this));
    dispatcher.register('onRedoActions', this.redoActions.bind(this));
  }

  setDefaults() {
    let defaultState = {
      nodes: [],
      connections: [],
      comments: [],
      activeNodeId: null,
      currentSourceCount: 0,
      currentTransformCount: 0,
      currentSinkCount: 0,
      canvasPanning: {
        top: 0,
        left: 0
      },
    };
    this.state = Object.assign({}, defaultState);
    this.stateHistory = {
      past: [],
      future: []
    };
  }

  reset() {
    this.changeListeners = [];
    this.setDefaults();
  }

  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }

  addSourceCount() {
    this.state.currentSourceCount++;
  }
  addTransformCount() {
    this.state.currentTransformCount++;
  }
  addSinkCount() {
    this.state.currentSinkCount++;
  }
  resetSourceCount() {
    this.state.currentSourceCount = 0;
  }
  resetTransformCount() {
    this.state.currentTransformCount = 0;
  }
  resetSinkCount() {
    this.state.currentSinkCount = 0;
  }

  resetPluginCount() {
    this.state.currentSourceCount = 0;
    this.state.currentTransformCount = 0;
    this.state.currentSinkCount = 0;
  }

  setCanvasPanning(panning) {
    this.state.canvasPanning.top = panning.top;
    this.state.canvasPanning.left = panning.left;
  }

  getSourceCount() {
    return this.state.currentSourceCount;
  }
  getTransformCount() {
    return this.state.currentTransformCount;
  }
  getSinkCount() {
    return this.state.currentSinkCount;
  }
  getCanvasPanning() {
    return this.state.canvasPanning;
  }

  addNode(nodeConfig) {
    if (!nodeConfig.name) {
      nodeConfig.name = nodeConfig.plugin.label + '-' + this.uuid.v4();
    }
    this.addStateToHistory();
    switch (this.GLOBALS.pluginConvert[nodeConfig.type]) {
      case 'source':
        this.addSourceCount();
        break;
      case 'transform':
        this.addTransformCount();
        break;
      case 'sink':
        this.addSinkCount();
        break;
    }
    this.state.nodes.push(nodeConfig);
    this.emitChange();
  }
  updateNode(nodeId, config) {
    var matchNode = this.state.nodes.filter( node => node.name === nodeId);
    if (!matchNode.length) {
      return;
    }
    this.addStateToHistory();
    matchNode = matchNode[0];
    angular.extend(matchNode, config);
    this.emitChange();
  }
  removeNode(node) {
    let match = this.state.nodes.filter(n => n.name === node);
    this.addStateToHistory();
    switch (this.GLOBALS.pluginConvert[match[0].type]) {
      case 'source':
        this.resetSourceCount();
        break;
      case 'transform':
        this.resetTransformCount();
        break;
      case 'sink':
        this.resetSinkCount();
        break;
    }
    this.state.nodes.splice(this.state.nodes.indexOf(match[0]), 1);
    this.state.activeNodeId = null;
    this.emitChange();
  }
  getNodes() {
    return this.state.nodes;
  }
  getNodesAsObjects() {
    var obj = {};
    angular.forEach(this.state.nodes, function (node) {
      obj[node.name] = node;
    });
    return obj;
  }

  setNodes(nodes) {
    nodes.forEach(node => {
      if (!node.name) {
        node.name = node.label + '-' + this.uuid.v4();
      }
    });
    this.state.nodes = nodes;
    this.emitChange();
  }
  getActiveNodeId() {
    return this.state.activeNodeId;
  }
  setActiveNodeId(nodeId) {
    this.state.activeNodeId = nodeId;
    this.emitChange();
  }
  resetActiveNode() {
    this.state.activeNodeId = null;
    angular.forEach(this.state.nodes, (node) => {
      node.selected = false;
    });
    this.emitChange();
  }

  addConnection(connection) {
    this.addStateToHistory();
    this.state.connections.push(connection);
    this.emitChange();
  }
  updateConnections(connections) {
    this.addStateToHistory();
    this.state.connections = connections;
    this.emitChange();
  }
  removeConnection(connection) {
    this.addStateToHistory();
    let index = this.state.connections.indexOf(connection);
    this.state.connections.splice(index, 1);
    this.emitChange();
  }
  getConnections() {
    return this.state.connections;
  }
  setConnections(connections) {
    this.state.connections = connections;
    this.emitChange();
  }

  setNodesAndConnections(nodes, connections, comments) {
    this.setNodes(nodes);
    this.state.connections = connections;
    this.state.comments = comments ? comments : [];
    this.emitChange();
  }

  addComment(comment) {
    this.addStateToHistory();
    this.state.comments.push(comment);
    this.emitChange();
  }

  setComments(comments) {
    this.state.comments = comments;
    this.emitChange();
  }

  deleteComment(comment) {
    this.addStateToHistory();
    let index = this.state.comments.indexOf(comment);
    if (index > -1) {
      this.state.comments.splice(index, 1);
      this.emitChange();
    }
  }

  updateComment(commentId, config) {
    let matchComment = this.state.comments.filter( comment => comment.id === commentId);
    if (!matchComment.length) {
      return;
    }
    this.addStateToHistory();
    matchComment = matchComment[0];
    angular.extend(matchComment, config);
    this.emitChange();
  }

  getComments() {
    return this.state.comments;
  }

  setState(state) {
    this.state = state;
    this.emitChange();
  }

  addStateToHistory() {
    let currentState = angular.copy(this.state);
    this.stateHistory.past.push(currentState);
    this.stateHistory.future = [];
  }

  undoActions() {
    let past = this.stateHistory.past;
    if (past.length > 0) {
      let previousState = past[past.length - 1];
      let presentState = angular.copy(this.state);
      this.stateHistory.past = past.slice(0, past.length - 1);
      this.stateHistory.future.unshift(presentState);
      this.setState(previousState);
    }
  }

  redoActions() {
    let future = this.stateHistory.future;
    if (future.length > 0) {
      let nextState = future[0];
      let presentState = angular.copy(this.state);
      this.stateHistory.past.push(presentState);
      this.stateHistory.future.shift();
      this.setState(nextState);
    }
  }
}

DAGPlusPlusNodesStore.$inject = ['DAGPlusPlusNodesDispatcher', 'uuid', 'GLOBALS'];
angular.module(`${PKG.name}.commons`)
  .service('DAGPlusPlusNodesStore', DAGPlusPlusNodesStore);
