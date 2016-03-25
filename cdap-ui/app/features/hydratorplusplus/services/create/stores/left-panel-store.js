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

var uniquePluginFilter = (typeMap) => {
  return (plugin) => {
    typeMap[plugin.name] = typeMap[plugin.name] || [];
    if (typeMap[plugin.name].length) {
      typeMap[plugin.name].push(plugin);
      return false;
    }
    typeMap[plugin.name].push(plugin);
    return true;
  };
};

var updateDefaultVersion = (pluginsList, defaultArtifactMap = {}) => {
  if (!Object.keys(defaultArtifactMap).length) {
    return;
  }
  pluginsList.forEach((plugin) => {
    let key = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;
    if(defaultArtifactMap.hasOwnProperty(key)) {
      plugin.defaultArtifact = defaultArtifactMap[key];
    }
  });
};

var mapPluginsWithMoreInfo = (type, typeMap, DAGPlusPlusFactory, popoverTemplate) => {
  return (plugin) => {
    plugin.type = type;
    plugin.icon = DAGPlusPlusFactory.getIcon(plugin.name);
    plugin.template = popoverTemplate;
    plugin.defaultArtifact = typeMap[plugin.name][0].artifact;
    plugin.allArtifacts = typeMap[plugin.name].map( (plugin) => plugin.artifact);
    return plugin;
  };
};

var mapPluginTemplatesWithMoreInfo = (type, DAGPlusPlusFactory, popoverTemplate) => {
  return (plugin) => {
    plugin.type = type;
    plugin.icon = DAGPlusPlusFactory.getIcon(plugin.pluginName);
    plugin.template = popoverTemplate;

    return plugin;
  };
};

class HydratorPlusPlusLeftPanelStore {
  constructor(HydratorPlusPlusLeftPanelDispatcher, HydratorPlusPlusPluginsDispatcher, DAGPlusPlusFactory, GLOBALS, HydratorPlusPlusConfigStore, mySettings, $q, $timeout) {
    this.state = {};
    this.setDefaults();
    this.DAGPlusPlusFactory = DAGPlusPlusFactory;
    this.changeListeners = [];
    this.sourcesToVersionMap = {};
    this.transformsToVersionMap = {};
    this.sinksToVersionMap = {};
    this.popoverTemplate = '/assets/features/hydratorplusplus/templates/create/popovers/leftpanel-plugin-popover.html';
    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.mySettings = mySettings;
    this.$q = $q;
    this.$timeout = $timeout;

    this.mySettings
        .get('plugin-default-version')
        .then((res = {}) => {
          this.state.defaultArtifactMap = res;
          return this.$q.resolve;
        })
        .then(this.cleanupNonExistantPlugins.bind(this));

    let dispatcher = HydratorPlusPlusLeftPanelDispatcher.getDispatcher();
    dispatcher.register('onLeftPanelToggled', this.setState.bind(this));
    dispatcher.register('toggleLeftPanelState', this.togglePanelState.bind(this));

    let hydratorPlusPlusPluginsDispatcher = HydratorPlusPlusPluginsDispatcher.getDispatcher();
    hydratorPlusPlusPluginsDispatcher.register('onArtifactsFetch', this.setArtifacts.bind(this));
    hydratorPlusPlusPluginsDispatcher.register('onSourcesFetch', this.setSources.bind(this));
    hydratorPlusPlusPluginsDispatcher.register('onTransformsFetch', this.setTransforms.bind(this));
    hydratorPlusPlusPluginsDispatcher.register('onSinksFetch', this.setSinks.bind(this));
    hydratorPlusPlusPluginsDispatcher.register('onPluginTemplatesFetch', this.updatePluginTemplates.bind(this));
  }
  setDefaults() {
    this.state = {
      panelState: true,
      plugins: {},
      artifacts: [],
      defaultArtifactMap: {},
      pluginTemplates: {
        source: [],
        transform: [],
        sink: []
      }
    };
  }

  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }

  getState() {
    return this.state.panelState;
  }
  setState(state) {
    this.state = state;
    this.emitChange();
  }
  togglePanelState() {
    this.state.panelState = !this.state.panelState;
    this.emitChange();
  }

  setSources(plugins, type) {
    this.sourcesToVersionMap = {};
    this.state.plugins.sources = plugins.filter(uniquePluginFilter(this.sourcesToVersionMap)).map(mapPluginsWithMoreInfo(type, this.sourcesToVersionMap, this.DAGPlusPlusFactory, this.popoverTemplate));
    this.checkAndUpdateDefaultVersion(this.state.plugins.sources);
    this.emitChange();
  }
  getSources() {
    let sources = angular.copy(this.state.plugins.sources) || [];
    let templates = angular.copy(this.state.pluginTemplates.source);
    return sources.concat(templates);
  }

  setTransforms(plugins, type) {
    this.transformsToVersionMap = {};
    this.state.plugins.transforms = plugins.filter(uniquePluginFilter(this.transformsToVersionMap)).map(mapPluginsWithMoreInfo(type, this.transformsToVersionMap, this.DAGPlusPlusFactory, this.popoverTemplate));
    this.checkAndUpdateDefaultVersion(this.state.plugins.transforms);
    this.emitChange();
  }
  getTransforms() {
    let transforms = angular.copy(this.state.plugins.transforms) || [];
    let templates = angular.copy(this.state.pluginTemplates.transform);
    return transforms.concat(templates);
  }

  setSinks(plugins, type) {
    this.sinksToVersionMap = {};
    this.state.plugins.sinks = plugins.filter(uniquePluginFilter(this.sinksToVersionMap)).map(mapPluginsWithMoreInfo(type, this.sinksToVersionMap, this.DAGPlusPlusFactory, this.popoverTemplate));
    this.checkAndUpdateDefaultVersion(this.state.plugins.sinks);
    this.emitChange();
  }
  getSinks() {
    let sinks = angular.copy(this.state.plugins.sinks) || [];
    let templates = angular.copy(this.state.pluginTemplates.sink);
    return sinks.concat(templates);
  }

  setArtifacts(artifacts) {
    this.state.artifacts = artifacts;
    this.emitChange();
  }
  getArtifacts() {
    let UISupportedArtifacts = ['cdap-etl-batch', 'cdap-etl-realtime'];
    return this.state.artifacts.filter(artifact => UISupportedArtifacts.indexOf(artifact.name) !== -1);
  }

  checkAndUpdateDefaultVersion(pluginsList) {
    if (!angular.isObject(this.state.defaultVersionsMap)) {
      this.mySettings
          .get('plugin-default-version')
          .then( res => {
            this.state.defaultVersionsMap = res;
            updateDefaultVersion(pluginsList, this.state.defaultVersionsMap);
          });
    } else {
      updateDefaultVersion(pluginsList, this.state.defaultVersionsMap);
    }
  }
  updatePluginDefaultVersion(plugin) {
    var key = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;
    if (this.state.defaultArtifactMap.hasOwnProperty(key)) {
      if (!angular.equals(this.state.defaultArtifactMap[key], plugin.artifact)) {
        this.state.defaultArtifactMap[key] = plugin.artifact;
        this.mySettings.set('plugin-default-version', this.state.defaultArtifactMap);
      }
    } else {
      this.state.defaultArtifactMap[key] = plugin.artifact;
      this.mySettings.set('plugin-default-version', this.state.defaultArtifactMap);
    }
  }
  getSpecificPluginVersion(plugin) {
    if (plugin.pluginTemplate) {
      return plugin;
    }

    var typeMap;
    var pluginTypes = this.GLOBALS.pluginTypes[this.HydratorPlusPlusConfigStore.getAppType()];
    switch(plugin.type) {
      case pluginTypes.source:
        typeMap = this.sourcesToVersionMap;
        break;
      case pluginTypes.sink:
        typeMap = this.sinksToVersionMap;
        break;
      case pluginTypes.transform:
        typeMap = this.transformsToVersionMap;
        break;
    }
    if (!typeMap) {
      return;
    }
    return typeMap[plugin.name].filter( plug => {
      if (angular.equals(plugin.defaultArtifact, plug.artifact)) {
        plug.icon = plugin.icon;
        plug.type = plugin.type;
        return true;
      }
      return false;
    })[0];
  }
  cleanupNonExistantPlugins() {
    let defaultVersionsMap = angular.copy(this.state.defaultArtifactMap);
    let defaultVersionChange = [];
    if (!angular.isArray(this.state.plugins.sources) && !angular.isArray(this.state.plugins.sinks) && !angular.isArray(this.state.plugins.transforms)) {
      this.$timeout(this.cleanupNonExistantPlugins.bind(this));
      return;
    }
    this.state.plugins.sources
        .concat(this.state.plugins.sinks)
        .concat(this.state.plugins.transforms)
        .forEach( plugin => {
          let key = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;
          if (defaultVersionsMap.hasOwnProperty(key)) {
            let matchingArtifact = plugin.allArtifacts.filter( artifact => angular.equals(artifact, defaultVersionsMap[key]));
            if (matchingArtifact.length) {
              defaultVersionChange.push({name: plugin.name, type: plugin.type});
              delete defaultVersionsMap[key];
            }
          }
        });
    if (Object.keys(defaultVersionsMap).length) {
      angular.forEach(defaultVersionsMap, (pluginArtifact, pluginKey) => {
        delete this.state.defaultVersionsMap[pluginKey];
      });
      let pipelineType = this.HydratorPlusPlusConfigStore.getAppType();
      this.mySettings.set('plugin-default-version', this.state.defaultVersionsMap);
      this.setSources(this.state.plugins.sources, this.GLOBALS.pluginTypes[pipelineType]['source']);
      this.setSinks(this.state.plugins.sinks, this.GLOBALS.pluginTypes[pipelineType]['sink']);
      this.setTransforms(this.state.plugins.transforms, this.GLOBALS.pluginTypes[pipelineType]['transform']);
    }
  }

  updatePluginTemplates(plugins, params) {
    let pipelineType = this.HydratorPlusPlusConfigStore.getAppType();
    if (!plugins || !plugins[params.namespace] || !plugins[params.namespace][pipelineType]) { return; }

    let pluginsList = plugins[params.namespace][pipelineType];
    angular.forEach(pluginsList, (plugins, key) => {
      this.state.pluginTemplates[this.GLOBALS.pluginConvert[key]] = _.values(plugins).map(mapPluginTemplatesWithMoreInfo(key, this.DAGPlusPlusFactory, this.popoverTemplate));
    });

    this.emitChange();
  }
}

HydratorPlusPlusLeftPanelStore.$inject = ['HydratorPlusPlusLeftPanelDispatcher', 'HydratorPlusPlusPluginsDispatcher', 'DAGPlusPlusFactory', 'GLOBALS', 'HydratorPlusPlusConfigStore', 'mySettings', '$q', '$timeout'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .service('HydratorPlusPlusLeftPanelStore', HydratorPlusPlusLeftPanelStore);
