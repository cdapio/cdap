/*
 * Copyright © 2017 Cask Data, Inc.
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

const getPluginToArtifactMap = (plugins = []) => {
  let typeMap = {};
  plugins.forEach( plugin => {
    typeMap[plugin.name] = typeMap[plugin.name] || [];
    typeMap[plugin.name].push(plugin);
  });
  return typeMap;
};

const getDefaultVersionForPlugin = (plugin = {}, defaultVersionMap = {}) => {
  let defaultVersionsList = Object.keys(defaultVersionMap);
  let key = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;
  let isDefaultVersionExists = defaultVersionsList.indexOf(key) !== -1;

  let isArtifactExistsInBackend = (plugin.allArtifacts || []).filter(plug => angular.equals(plug.artifact, defaultVersionMap[key]));
  if (!isDefaultVersionExists || !isArtifactExistsInBackend.length) {
    return plugin.artifact;
  }

  return angular.copy(defaultVersionMap[key]);
};

const popoverTemplate = '/assets/features/hydrator/templates/create/popovers/leftpanel-plugin-popover.html';

class PipelineAvailablePluginsActions {
  constructor(myPipelineApi, GLOBALS, $q, AvailablePluginsStore, AVAILABLE_PLUGINS_ACTIONS, DAGPlusPlusFactory, $filter, myHelpers, LEFTPANELSTORE_ACTIONS, HydratorPlusPlusLeftPanelStore, mySettings) {
    this.api = myPipelineApi;
    this.GLOBALS = GLOBALS;
    this.$q = $q;
    this.store = AvailablePluginsStore;
    this.actions = AVAILABLE_PLUGINS_ACTIONS;
    this.DAGPlusPlusFactory = DAGPlusPlusFactory;
    this.$filter = $filter;
    this.myHelpers = myHelpers;
    this.leftpanelactions = LEFTPANELSTORE_ACTIONS;
    this.leftpanelstore = HydratorPlusPlusLeftPanelStore;
    this.mySettings = mySettings;
  }

  fetchPlugins(extensionsParams) {
    this.api.fetchExtensions(extensionsParams)
      .$promise
      .then((res) => {
        // filter out extensions
        const extensionsList = this.GLOBALS.pluginTypes[extensionsParams.pipelineType];

        const extensionMap = Object.keys(extensionsList).map((ext) => {
          return extensionsList[ext];
        });

        let supportedExtensions = res.filter((ext) => {
          return extensionMap.indexOf(ext) !== -1;
        });

        this._fetchPlugins(extensionsParams, supportedExtensions);
      }, (err) => {
        console.log('ERR: Fetching list of artifacts failed', err);
      });
  }

  fetchPluginsForDetails(namespace, stages) {
    let availablePluginsMap = {};
    let pluginsList = [];

    stages.forEach((stage) => {
      let pluginInfo = this._createPluginInfo(stage.plugin);
      availablePluginsMap[pluginInfo.key] = {
        pluginInfo: stage.plugin
      };

      pluginsList.push(pluginInfo.info);
    });

    this._fetchInfo(availablePluginsMap, namespace, pluginsList);
  }

  _fetchPlugins(params, extensions) {
    let fetchList = [];

    extensions.forEach((ext) => {
      let fetchParams = Object.assign({}, params, {
        extensionType: ext
      });
      let fetchApi = this.api.fetchPlugins(fetchParams).$promise;
      fetchList.push(fetchApi);
    });

    this.$q.all(fetchList)
      .then((res) => {
        this._prepareInfoRequest(params.namespace, res);
        this._formatPluginsResponse(res, extensions, params.namespace, params.pipelineType);
      }, (err) => {
        console.log('ERR: Fetching plugins', err);
      });
  }

  _fetchInfo(availablePluginsMap, namespace, plugins) {
    this.api.fetchAllPluginsProperties({ namespace }, plugins)
      .$promise
      .then((res) => {
        res.forEach((plugin) => {
          let pluginProperties = Object.keys(plugin.properties);
          if (pluginProperties.length === 0) { return; }

          let pluginKey = pluginProperties[0].split('.')[1];
          let key = `${pluginKey}-${plugin.name}-${plugin.version}-${plugin.scope}`;

          availablePluginsMap[key].doc = plugin.properties[`doc.${pluginKey}`];

          let parsedWidgets;
          let widgets = plugin.properties[`widgets.${pluginKey}`];

          if (widgets) {
            try {
              parsedWidgets = JSON.parse(widgets);
            } catch (e) {
              console.log('failed to parse widgets', e, pluginKey);
            }
          }
          availablePluginsMap[key].widgets = parsedWidgets;
        });

        this.store.dispatch({
          type: this.actions.setPluginsMap,
          payload: {
            pluginsMap: availablePluginsMap
          }
        });
      });
  }

  _createPluginInfo(plugin) {
    let pluginKey = `${plugin.name}-${plugin.type}`;

    let availablePluginKey = `${plugin.name}-${plugin.type}-${plugin.artifact.name}-${plugin.artifact.version}-${plugin.artifact.scope}`;

    let info = Object.assign({}, plugin.artifact, {
      properties: [
        `widgets.${pluginKey}`,
        `doc.${pluginKey}`
      ]
    });

    return {
      info,
      key: availablePluginKey
    };
  }

  _prepareInfoRequest(namespace, pluginsList) {
    // Create request body for artifactproperties batch call
    let plugins = [];
    let availablePluginsMap = {};

    pluginsList.forEach((extension) => {
      extension.forEach((plugin) => {
        let pluginInfo = this._createPluginInfo(plugin);
        availablePluginsMap[pluginInfo.key] = {
          pluginInfo: plugin
        };

        plugins.push(pluginInfo.info);
      });
    });

    this._fetchInfo(availablePluginsMap, namespace, plugins);
  }

  _getPluginsWithAddedInfo(plugins = [], pluginToArtifactArrayMap = {}, extension = '') {
    if ([plugins.length, extension.length].indexOf(0) !== -1) {
      return plugins;
    }

    const getExtraProperties = (plugin = {}, extension = '') => {
      return Object.assign({}, {
        type: extension,
        icon: this.DAGPlusPlusFactory.getIcon(plugin.name || plugin.pluginName),
        label: this.$filter('myRemoveCamelcase')(plugin.name || plugin.pluginName),
        template: popoverTemplate
      });
    };

    const getAllArtifacts = (_pluginToArtifactArrayMap = {}, plugin = {}, extension = '') => {
      if ([Object.keys(_pluginToArtifactArrayMap).length, Object.keys(plugin).length].indexOf(0) !== -1) {
        return [];
      }
      let _pluginArtifacts = _pluginToArtifactArrayMap[(plugin.name || plugin.pluginName)];
      if (!Array.isArray(_pluginArtifacts)) {
        return [];
      }
      return [..._pluginArtifacts]
             .map( plug => Object.assign({}, plug, getExtraProperties(plug, extension)));
    };

    const getArtifact = (_pluginToArtifactArrayMap = {}, plugin = {}) => {
      if(!Object.keys(plugin).length) { return {}; }
      return this.myHelpers.objectQuery(_pluginToArtifactArrayMap, (plugin.name || plugin.pluginName), 0, 'artifact') || plugin.artifact;
    };

    return Object.keys(pluginToArtifactArrayMap).map( pluginName => {
      let plugin = pluginToArtifactArrayMap[pluginName][0];
      return Object.assign({}, plugin, getExtraProperties(plugin, extension), {
        artifact: getArtifact(pluginToArtifactArrayMap, plugin),
        allArtifacts: getAllArtifacts(pluginToArtifactArrayMap, plugin, extension)
      });
    });
  }

  _formatPluginsResponse(pluginsList, extensions, namespace, pipelineType) {
    let pluginTypes = {};

    extensions.forEach((ext, i) => {
      let plugins = pluginsList[i];

      let pluginToArtifactArrayMap = getPluginToArtifactMap(plugins);
      let pluginsWithAddedInfo = this._getPluginsWithAddedInfo(plugins, pluginToArtifactArrayMap, ext);

      // Fetch default version
      let versionMap = this.leftpanelstore.getState().plugins.pluginToVersionMap;

      pluginTypes[ext] = pluginsWithAddedInfo.map((plugin) => {
        plugin.defaultArtifact = getDefaultVersionForPlugin(plugin, versionMap);
        return plugin;
      });
    });

    this.leftpanelstore.dispatch({
      type: this.leftpanelactions.FETCH_ALL_PLUGINS,
      payload: {
        pluginTypes,
        extensions
      }
    });

    this.leftpanelstore.dispatch({
      type: this.leftpanelactions.PLUGIN_DEFAULT_VERSION_CHECK_AND_UPDATE
    });

    this._fetchTemplates(namespace, pipelineType);
  }

  _fetchTemplates(namespace, pipelineType) {
    this.mySettings
        .get('pluginTemplates')
        .then(res => {
          if (!res) { return; }

          this.leftpanelstore.dispatch({
            type: this.leftpanelactions.PLUGIN_TEMPLATE_FETCH,
            payload: { res, pipelineType, namespace }
          });
        });
  }
}

angular.module(`${PKG.name}.feature.hydrator`)
  .service('PipelineAvailablePluginsActions', PipelineAvailablePluginsActions);
