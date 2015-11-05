class PipelineCreateDAGActionsFactory{
  constructor(NodeConfigDispatcher) {
    this.nodeConfigDispatcher = NodeConfigDispatcher.getDispatcher();
  }
  addNode(node) {
    this.nodeConfigDispatcher.dispatch('onPluginAdd', node);
  }
  removeNode(node) {
    this.nodeConfigDispatcher.dispatch('onPluginRemove', node);
  }
  choosNode(node) {
    this.nodeConfigDispatcher.dispatch('onPluginChoose', node);
  }
}
PipelineCreateDAGActionsFactory.$inject = ['NodeConfigDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('PipelineCreateDAGActionsFactory', PipelineCreateDAGActionsFactory);
