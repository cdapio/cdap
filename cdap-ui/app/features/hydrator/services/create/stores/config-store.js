class ConfigStore {
  // constructor(MetadataDispatcher, NodeConfigDispatcher, DAGEventDispatcher) {
  constructor(){
    this.state = {};
    this.setDefaults();
    // var metadataDispatcher = MetadataDispatcher.getDispatcher();
    // var nodeConfigDispatcher = NodeConfigDispatcher.getDispatcher();
    // var dagEventDispatcher = DAGEventDispatcher.getDispatcher();
    // Have to figure out what dispatchers should do what.
  }
  setDefaults() {
    this.state = {
      artifact: {
        name: '',
        scope: 'SYSTEM',
        version: ''
      },
      config: {
        source: {},
        sinks: [],
        transforms: []
      },
      description: ''
    };
  }

  setState(state) {
    this.state = state;
  }

  getState() {

  }
  getArtifact() {
    return this.state.artifact;
  }
  getConfig() {
    return this.state.config;
  }
  getDescription() {
    return this.state.description;
  }
  setDescription(description) {
    this.state.description = description;
  }
  setConfig(config, type) {
    switch(type) {
      case 'source':
        this.state.source = config;
        break;
      case 'sink':
        this.state.sinks.push(config);
        break;
      case 'transform':
        this.state.transforms.push(config);
        break;
    }
  }
  setArtifact(artifact) {
    this.state.artifact.name = artifact.name;
    this.state.artifact.version = artifact.version;
  }
}
ConfigStore.$inject = ['MetadataDispatcher', 'NodeConfigDispatcher', 'DAGEventDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigStore', ConfigStore);
