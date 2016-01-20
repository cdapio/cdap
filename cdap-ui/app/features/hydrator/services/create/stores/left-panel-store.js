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

var updateDefaultVersion = (pluginsList, defaultVersionMap = {}) => {
  if (!Object.keys(defaultVersionMap).length) {
    return;
  }
  pluginsList.forEach((plugin) => {
    let key = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;
    if(defaultVersionMap.hasOwnProperty(key)) {
      plugin.defaultVersion = defaultVersionMap[key];
    }
  });
};

var mapPluginsWithMoreInfo = (type, typeMap, MyDAGFactory, popoverTemplate) => {
  return (plugin) => {
    plugin.type = type;
    plugin.icon = MyDAGFactory.getIcon(plugin.name);
    plugin.template = popoverTemplate;
    plugin.defaultVersion = typeMap[plugin.name][0].artifact.version;
    plugin.allVersions = typeMap[plugin.name].map( (plugin) => plugin.artifact.version);
    return plugin;
  };
};

var mapPluginTemplatesWithMoreInfo = (type, MyDAGFactory, popoverTemplate) => {
  return (plugin) => {
    plugin.type = type;
    plugin.icon = MyDAGFactory.getIcon(plugin.pluginName);
    plugin.template = popoverTemplate;

    return plugin;
  };
};

class LeftPanelStore {
  constructor(LeftPanelDispatcher, PluginsDispatcher, MyDAGFactory, GLOBALS, ConfigStore, mySettings, $q, $timeout) {
    this.state = {};
    this.setDefaults();
    this.MyDAGFactory = MyDAGFactory;
    this.changeListeners = [];
    this.sourcesToVersionMap = {};
    this.transformsToVersionMap = {};
    this.sinksToVersionMap = {};
    this.popoverTemplate = '/assets/features/hydrator/templates/create/popovers/leftpanel-plugin-popover.html';
    this.GLOBALS = GLOBALS;
    this.ConfigStore = ConfigStore;
    this.mySettings = mySettings;
    this.$q = $q;
    this.$timeout = $timeout;

    this.mySettings
        .get('plugin-default-version')
        .then((res = {}) => {
          this.state.defaultVersionsMap = res;
          return this.$q.resolve;
        })
        .then(this.cleanupNonExistantPlugins.bind(this));

    let dispatcher = LeftPanelDispatcher.getDispatcher();
    dispatcher.register('onLeftPanelToggled', this.setState.bind(this));
    dispatcher.register('toggleLeftPanelState', this.togglePanelState.bind(this));

    let pluginsDispatcher = PluginsDispatcher.getDispatcher();
    pluginsDispatcher.register('onSourcesFetch', this.setSources.bind(this));
    pluginsDispatcher.register('onTransformsFetch', this.setTransforms.bind(this));
    pluginsDispatcher.register('onSinksFetch', this.setSinks.bind(this));
    pluginsDispatcher.register('onPluginTemplatesFetch', this.updatePluginTemplates.bind(this));
  }
  setDefaults() {
    this.state = {
      panelState: true,
      plugins: {},
      defaultVersionsMap: null,
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
    this.state.plugins.sources = plugins.filter(uniquePluginFilter(this.sourcesToVersionMap)).map(mapPluginsWithMoreInfo(type, this.sourcesToVersionMap, this.MyDAGFactory, this.popoverTemplate));
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
    this.state.plugins.transforms = plugins.filter(uniquePluginFilter(this.transformsToVersionMap)).map(mapPluginsWithMoreInfo(type, this.transformsToVersionMap, this.MyDAGFactory, this.popoverTemplate));
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
    this.state.plugins.sinks = plugins.filter(uniquePluginFilter(this.sinksToVersionMap)).map(mapPluginsWithMoreInfo(type, this.sinksToVersionMap, this.MyDAGFactory, this.popoverTemplate));
    this.checkAndUpdateDefaultVersion(this.state.plugins.sinks);
    this.emitChange();
  }
  getSinks() {
    let sinks = angular.copy(this.state.plugins.sinks) || [];
    let templates = angular.copy(this.state.pluginTemplates.sink);
    return sinks.concat(templates);
  }

  checkAndUpdateDefaultVersion(pluginsList) {
    if (!angular.isObject(this.state.defaultVersionsMap)) {
      this.mySettings
          .get('plugin-default-version')
          .then( res => {
            this.state.defaultVersionsMap = res;
            this.updateDefaultVersion(pluginsList, this.state.defaultVersionsMap);
          });
    } else {
      updateDefaultVersion(pluginsList, this.state.defaultVersionsMap);
    }
  }
  updatePluginDefaultVersion(plugin) {
    var key = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;
    if (this.state.defaultVersionsMap.hasOwnProperty(key)) {
      if (this.state.defaultVersionsMap[key] !== plugin.defaultVersion) {
        this.state.defaultVersionsMap[key] = plugin.defaultVersion;
        this.mySettings.set('plugin-default-version', this.state.defaultVersionsMap);
      }
    } else {
      this.state.defaultVersionsMap[key] = plugin.defaultVersion;
      this.mySettings.set('plugin-default-version', this.state.defaultVersionsMap);
    }
  }
  getSpecificPluginVersion(plugin) {
    if (plugin.pluginTemplate) {
      return plugin;
    }

    var typeMap;
    var pluginTypes = this.GLOBALS.pluginTypes[this.ConfigStore.getAppType()];
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
      if (plugin.defaultVersion === plug.artifact.version) {
        plug.icon = plugin.icon;
        plug.type = plugin.type;
        return true;
      }
      return false;
    })[0];
  }
  cleanupNonExistantPlugins() {
    let defaultVersionsMap = angular.copy(this.state.defaultVersionsMap);
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
            delete defaultVersionsMap[key];
          }
        });
    if (Object.keys(defaultVersionsMap).length) {
      angular.forEach(defaultVersionsMap, (pluginVersion, pluginKey) => {
        delete this.state.defaultVersionsMap[pluginKey];
      });
      this.mySettings.set('plugin-default-version', this.state.defaultVersionsMap);
    }
  }

  updatePluginTemplates(plugins, params) {
    let pipelineType = this.ConfigStore.getAppType();
    if (!plugins || !plugins[params.namespace] || !plugins[params.namespace][pipelineType]) { return; }

    let pluginsList = plugins[params.namespace][pipelineType];
    angular.forEach(pluginsList, (plugins, key) => {
      this.state.pluginTemplates[this.GLOBALS.pluginConvert[key]] = _.values(plugins).map(mapPluginTemplatesWithMoreInfo(key, this.MyDAGFactory, this.popoverTemplate));
    });

    this.emitChange();
  }
}

LeftPanelStore.$inject = ['LeftPanelDispatcher', 'PluginsDispatcher', 'MyDAGFactory', 'GLOBALS', 'ConfigStore', 'mySettings', '$q', '$timeout'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('LeftPanelStore', LeftPanelStore);
