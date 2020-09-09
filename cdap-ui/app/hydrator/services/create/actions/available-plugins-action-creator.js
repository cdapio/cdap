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

const popoverTemplate = '/assets/features/hydrator/templates/create/popovers/leftpanel-plugin-popover.html';

class PipelineAvailablePluginsActions {
  constructor(myPipelineApi, GLOBALS, $q, AvailablePluginsStore, AVAILABLE_PLUGINS_ACTIONS, DAGPlusPlusFactory, $filter, myHelpers, LEFTPANELSTORE_ACTIONS, HydratorPlusPlusLeftPanelStore, mySettings, HydratorPlusPlusNodeService) {
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
    this.hydratorNodeService = HydratorPlusPlusNodeService;
  }

  fetchPlugins(extensionsParams, promise) {
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

        this._fetchPlugins(extensionsParams, supportedExtensions, promise);
      }, (err) => {
        if (promise) {
          promise.reject(err);
        }
        console.log('ERR: Fetching list of artifacts failed', err);
      });
  }

  fetchPluginsForDetails(namespace, stages) {
    let availablePluginsMap = {};
    let pluginsList = [];

    this.api.getAllArtifacts({ namespace })
      .$promise
      .then((res) => {
        // create map for all available artifacts
        const artifactsMap = {};
        res.forEach((artifact) => {
          const artifactKey = this._getArtifactKey(artifact);
          artifactsMap[artifactKey] = artifact;
        });

        stages.forEach((stage) => {
          const stageArtifact = this.myHelpers.objectQuery(stage, 'plugin', 'artifact');
          const artifactKey = this._getArtifactKey(stageArtifact);
          if (!artifactsMap[artifactKey]) {
            return;
          }

          let pluginInfo = this._createPluginInfo(stage.plugin);
          availablePluginsMap[pluginInfo.key] = {
            pluginInfo: stage.plugin
          };

          pluginsList.push(pluginInfo);
        });

        this._fetchInfo(availablePluginsMap, namespace, pluginsList);
      });


  }

  fetchPluginsForUpgrade(extensionsParams) {
    let deferred = this.$q.defer();

    this.fetchPlugins(extensionsParams, deferred);

    return deferred.promise;
  }

  _fetchPlugins(params, extensions, promise) {
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
        let pluginTypes = this._formatPluginsResponse(res, extensions);

        if (promise) {
          promise.resolve(pluginTypes);
          return;
        }

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

        this._prepareInfoRequest(params.namespace, res);
        this._fetchTemplates(params.namespace, params.pipelineType);

      }, (err) => {
        if (promise) {
          promise.reject(err);
        }
        console.log('ERR: Fetching plugins', err);
      });
  }

  _fetchInfo(availablePluginsMap, namespace, plugins) {
    const reqBody = plugins.map((plugin) => plugin.info);

    const getKeyFromPluginProps = (pluginProperties) => {
      const key = this.myHelpers.objectQuery(pluginProperties, '0');
      return key ? key.split('.')[1] : '';
    };

    this.api.fetchAllPluginsProperties({ namespace }, reqBody)
      .$promise
      .then((res) => {
        res.forEach((plugin) => {
          const pluginProperties = Object.keys(plugin.properties);
          if (pluginProperties.length === 0) { return; }

          const pluginKey = getKeyFromPluginProps(pluginProperties);
          const key = `${pluginKey}-${this._getArtifactKey(plugin)}`;

          availablePluginsMap[key].doc = plugin.properties[`doc.${pluginKey}`];

          let parsedWidgets;
          const widgets = plugin.properties[`widgets.${pluginKey}`];

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
    const pluginKey = `${plugin.name}-${plugin.type}`;
    const availablePluginKey = `${pluginKey}-${this._getArtifactKey(plugin.artifact)}`;

    const info = Object.assign({}, plugin.artifact, {
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

  /**
   *
   * @param { name, version, scope } artifact
   */
  _getArtifactKey(artifact) {
    return `${artifact.name}-${artifact.version}-${artifact.scope}`;
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

        plugins.push(pluginInfo);
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
      const allPluginVersions = _pluginToArtifactArrayMap[plugin.name];
      const highestVersion = window.CaskCommon.VersionUtilities.findHighestVersion(allPluginVersions.map((plugin) => this.myHelpers.objectQuery(plugin, 'artifact', 'version')), true);
      const latestPluginVersion = allPluginVersions.find((plugin) => this.myHelpers.objectQuery(plugin, 'artifact', 'version') === highestVersion);
      return latestPluginVersion.artifact;
    };

    return Object.keys(pluginToArtifactArrayMap).map( pluginName => {
      let plugin = pluginToArtifactArrayMap[pluginName][0];
      return Object.assign({}, plugin, getExtraProperties(plugin, extension), {
        artifact: getArtifact(pluginToArtifactArrayMap, plugin),
        allArtifacts: getAllArtifacts(pluginToArtifactArrayMap, plugin, extension)
      });
    });
  }

  _formatPluginsResponse(pluginsList, extensions) {
    let pluginTypes = {};

    extensions.forEach((ext, i) => {
      let plugins = pluginsList[i];

      let pluginToArtifactArrayMap = this.hydratorNodeService.getPluginToArtifactMap(plugins);
      let pluginsWithAddedInfo = this._getPluginsWithAddedInfo(plugins, pluginToArtifactArrayMap, ext);

      // Fetch default version
      let versionMap = this.leftpanelstore.getState().plugins.pluginToVersionMap;

      pluginTypes[ext] = pluginsWithAddedInfo.map((plugin) => {
        plugin.defaultArtifact = this.hydratorNodeService.getDefaultVersionForPlugin(plugin, versionMap);
        return plugin;
      });
    });

    return pluginTypes;
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
