class ConfigStore {
  constructor(ConfigDispatcher){
    this.state = {};
    this.changeListeners = [];
    this.setDefaults();
    this.configDispatcher = ConfigDispatcher.getDispatcher();
    this.configDispatcher.register('onArtifactSave', this.setArtifact.bind(this));
  }
  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
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
      description: '',
      name: ''
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
  getName() {
    return this.state.name;
  }
  setName(name) {
    this.state.name = name;
    this.emitChange();
  }
  setDescription(description) {
    this.state.description = description;
    this.emitChange();
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
    this.emitChange();
  }
  setArtifact(artifact) {
    this.state.artifact.name = artifact.name;
    this.state.artifact.version = artifact.version;
    this.state.artifact.scope = artifact.scope;
    this.emitChange();
  }
}

ConfigStore.$inject = ['ConfigDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigStore', ConfigStore);
