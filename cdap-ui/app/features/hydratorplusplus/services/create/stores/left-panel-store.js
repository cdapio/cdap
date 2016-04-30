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
    plugin.name = plugin.pluginTemplate;
    if (plugin.pluginTemplate) {
      plugin.nodeClass = 'plugin-templates';
    }
    return plugin;
  };
};

class HydratorPlusPlusLeftPanelStore {
  constructor(HydratorPlusPlusLeftPanelDispatcher, HydratorPlusPlusPluginsDispatcher, DAGPlusPlusFactory, GLOBALS, HydratorPlusPlusConfigStore, mySettings, $q, $timeout) {
    this.state = {};
    this.setDefaults();
    this.DAGPlusPlusFactory = DAGPlusPlusFactory;
    this.changeListeners = [];
    this.pluginsToVersionMap = {};
    this.popoverTemplate = '/assets/features/hydratorplusplus/templates/create/popovers/leftpanel-plugin-popover.html';
    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.mySettings = mySettings;
    this.$q = $q;
    this.$timeout = $timeout;

    this.mySettings
        .get('plugin-default-version')
        .then((res = {}) => {
          this.state.defaultArtifactMap = res || {};
          return this.$q.resolve;
        })
        .then($timeout(this.cleanupNonExistentPlugins.bind(this), 10000));

    let hydratorPlusPlusPluginsDispatcher = HydratorPlusPlusPluginsDispatcher.getDispatcher();

    hydratorPlusPlusPluginsDispatcher.register('onReset', this.reset.bind(this));
    hydratorPlusPlusPluginsDispatcher.register('onExtensionsFetch', this.setExtensions.bind(this));
    hydratorPlusPlusPluginsDispatcher.register('onPluginsFetch', this.setPlugins.bind(this));
    hydratorPlusPlusPluginsDispatcher.register('onPluginTemplatesFetch', this.updatePluginTemplates.bind(this));
  }
  setDefaults() {
    this.state = {
      plugins: {},
      extensions: [], // Left panel store doesn't have a selected artifact as that information HAS to come from config store, that should be the only source of truth.
      defaultArtifactMap: {},
      pluginTemplates: {}
    };
  }

  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }

  reset() {
    this.changeListeners = [];
    this.setDefaults();
  }
  getState() {
    return this.state.panelState;
  }
  setState(state) {
    this.state = state;
    this.emitChange();
  }

  setExtensions(extensions) {
    let uiSupportedExtension = (extension) => {
      let pipelineType = this.HydratorPlusPlusConfigStore.getArtifact().name;
      let extensionMap = this.GLOBALS.pluginTypes[pipelineType];
      return Object.keys(extensionMap).filter(ext => extensionMap[ext] === extension).length;
    };
    extensions = extensions.filter(uiSupportedExtension);
    this.state.extensions = extensions;
    this.emitChange();
  }
  getExtensions() {
    return this.state.extensions;
  }

  setPlugins(extension, plugins) {
    this.pluginsToVersionMap[extension] = {};
    this.state.plugins[extension] = plugins
        .filter(
          uniquePluginFilter(this.pluginsToVersionMap[extension])
        )
        .map(
          mapPluginsWithMoreInfo(extension, this.pluginsToVersionMap[extension], this.DAGPlusPlusFactory, this.popoverTemplate)
        );
    this.checkAndUpdateDefaultVersion(this.state.plugins[extension]);
    this.emitChange();
  }

  getPlugins(extension) {
    return this.state.plugins[extension] || [];
  }
  checkAndUpdateDefaultVersion(pluginsList) {
    if (!angular.isObject(this.state.defaultArtifactMap)) {
      this.mySettings
          .get('plugin-default-version')
          .then( res => {
            this.state.defaultArtifactMap = res;
            updateDefaultVersion(pluginsList, this.state.defaultArtifactMap);
          });
    } else {
      updateDefaultVersion(pluginsList, this.state.defaultArtifactMap);
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
    let extension = plugin.type;
    let typeMap = this.pluginsToVersionMap[extension];
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
  cleanupNonExistentPlugins() {
    let defaultVersionsMap = angular.copy(this.state.defaultArtifactMap);
    let defaultVersionChange = [];
    this.getExtensions()
        .forEach(extension => {
          this.getPlugins(extension)
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
        });
    if (angular.isObject(defaultVersionsMap) && Object.keys(defaultVersionsMap).length) {
      angular.forEach(defaultVersionsMap, (pluginArtifact, pluginKey) => {
        delete this.state.defaultArtifactMap[pluginKey];
      });
      this.mySettings.set('plugin-default-version', this.state.defaultArtifactMap);
    }
  }

  getPluginTemplates(extension) {
    return this.state.pluginTemplates[extension] || [];
  }
  updatePluginTemplates(plugins, params) {
    let pipelineType = this.HydratorPlusPlusConfigStore.getAppType();
    if (!plugins || !plugins[params.namespace] || !plugins[params.namespace][pipelineType]) { return; }

    let pluginsList = plugins[params.namespace][pipelineType];
    angular.forEach(pluginsList, (plugins, key) => {
      this.state.pluginTemplates[key] = _.values(plugins).map(mapPluginTemplatesWithMoreInfo(key, this.DAGPlusPlusFactory, this.popoverTemplate));
    });
    this.emitChange();
  }
}

HydratorPlusPlusLeftPanelStore.$inject = ['HydratorPlusPlusLeftPanelDispatcher', 'HydratorPlusPlusPluginsDispatcher', 'DAGPlusPlusFactory', 'GLOBALS', 'HydratorPlusPlusConfigStore', 'mySettings', '$q', '$timeout'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .service('HydratorPlusPlusLeftPanelStore', HydratorPlusPlusLeftPanelStore);
