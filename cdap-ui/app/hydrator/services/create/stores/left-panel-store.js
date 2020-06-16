/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

/*
  This store is a collection of extensions and plugins for the side panel.
  {
    plugins: {
      'batchsource': {
        Stream:{
          type: ...,
          artifact: {...},
          allArtifacts: [ {...}, {...}],
          defaultArtifact
        }
      }
    },
    extensions: []
  }
*/

let leftpanelactions, _DAGPlusPlusFactory, _GLOBALS, _myHelpers, _filter, _hydratorNodeService;
let popoverTemplate = '/assets/features/hydrator/templates/create/popovers/leftpanel-plugin-popover.html';
let getInitialState = () => {
  return {
    plugins: {
      pluginTypes: {},
      pluginToVersionMap: {}
    },
    extensions: []
  };
};

const getTemplatesWithAddedInfo = (templates = [], extension = '') => {
  return templates.map( template => {
    return Object.assign({}, template, {
      nodeClass: 'plugin-templates',
      name: template.pluginTemplate,
      pluginName: template.pluginName,
      type: extension,
      icon: _DAGPlusPlusFactory.getIcon(template.pluginName),
      template: popoverTemplate,
      allArtifacts: [template.artifact]
    });
  });
};

const getPluginsWithAddedInfo = (plugins = [], pluginToArtifactArrayMap = {}, extension = '') => {
  if ([plugins.length, extension.length].indexOf(0) !== -1) {
    return plugins;
  }
  const getExtraProperties = (plugin = {}, extension = '') => {
    return Object.assign({}, {
      type: extension,
      icon: _DAGPlusPlusFactory.getIcon(plugin.name || plugin.pluginName),
      label: _filter('myRemoveCamelcase')(plugin.name || plugin.pluginName),
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
    if (!Object.keys(plugin).length) { return {}; }
    return _myHelpers.objectQuery(_pluginToArtifactArrayMap, (plugin.name || plugin.pluginName), 0, 'artifact') || plugin.artifact;
  };
  return Object.keys(pluginToArtifactArrayMap).map( pluginName => {
    let plugin = pluginToArtifactArrayMap[pluginName][0];
    return Object.assign({}, plugin, getExtraProperties(plugin, extension), {
      artifact: getArtifact(pluginToArtifactArrayMap, plugin),
      allArtifacts: getAllArtifacts(pluginToArtifactArrayMap, plugin, extension),
      pluginMapKey: `${plugin.name}-${plugin.type}-${plugin.artifact.name}-${plugin.artifact.version}-${plugin.artifact.scope}`
    });
  });
};

var plugins = (state = getInitialState().plugins, action = {}) => {
  let stateCopy;
  switch (action.type) {
    case leftpanelactions.PLUGINS_FETCH: {
      stateCopy = Object.assign({}, state);
      const { extension, plugins } = action.payload;

      const pluginToArtifactArrayMap = _hydratorNodeService.getPluginToArtifactMap(plugins);
      const pluginsWithAddedInfo = getPluginsWithAddedInfo(plugins, pluginToArtifactArrayMap, extension);

      stateCopy.pluginTypes[extension] = pluginsWithAddedInfo
        .map( plugin => {
          plugin.defaultArtifact = _hydratorNodeService.getDefaultVersionForPlugin(plugin, state.pluginToVersionMap);
          return plugin;
        })
        .concat((state.pluginTypes[extension] || []));

      stateCopy.pluginTypes = Object.assign({}, state.pluginTypes, stateCopy.pluginTypes);
      return Object.assign({}, state, stateCopy);
    }
    case leftpanelactions.FETCH_ALL_PLUGINS:
      stateCopy = Object.assign({}, state);

      stateCopy.pluginTypes = Object.assign({}, action.payload.pluginTypes);
      return Object.assign({}, state, stateCopy);

    case leftpanelactions.PLUGIN_TEMPLATE_FETCH: {
      stateCopy = Object.assign({}, state);
      const { pipelineType, namespace, res } = action.payload;
      const templatesList = _myHelpers.objectQuery(res, namespace, pipelineType);
      if (!templatesList) { return state; }

      angular.forEach( templatesList, (plugins, key) => {
        let _templates = _.values(plugins);
        let _pluginWithoutTemplates = (state.pluginTypes[key] || []).filter( plug => !plug.pluginTemplate);
        stateCopy.pluginTypes[key] = getTemplatesWithAddedInfo(_templates, key).concat(_pluginWithoutTemplates);
      });

      return Object.assign({}, state, stateCopy);
    }
    case leftpanelactions.PLUGINS_DEFAULT_VERSION_FETCH: {
      const defaultPluginVersionsMap = action.payload.res || {};
      stateCopy = Object.assign({}, getInitialState().plugins);
      if (Object.keys(defaultPluginVersionsMap).length) {
        const pluginTypes = Object.keys(state.pluginTypes);
        // If this is fetched after the all the plugins have been fetched from the backend then we will update them.
        pluginTypes.forEach( pluginType => {
          const _plugins = state.pluginTypes[pluginType];
          stateCopy.pluginTypes[pluginType] = _plugins
            .map( plugin => {
              plugin.defaultArtifact = _hydratorNodeService.getDefaultVersionForPlugin(plugin, defaultPluginVersionsMap);
              return plugin;
            });
        });
        stateCopy.pluginToVersionMap = defaultPluginVersionsMap;
        return Object.assign({}, state, stateCopy);
      }
      return state;
    }
    case leftpanelactions.PLUGIN_DEFAULT_VERSION_CHECK_AND_UPDATE: {
      let pluginTypes = Object.keys(state.pluginTypes);
      if (!pluginTypes.length) {
        return state;
      }
      let pluginToVersionMap = angular.copy(state.pluginToVersionMap);
      pluginTypes
        .forEach( pluginType => {
          state.pluginTypes[pluginType].forEach( plugin => {
            if (plugin.pluginTemplate) { return; }
            let key = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;
            let isArtifactExistsInBackend = plugin.allArtifacts.filter(
              plug => angular.equals(plug.artifact, pluginToVersionMap[key])
            );
            if (!isArtifactExistsInBackend.length) {
              delete pluginToVersionMap[key];
            }
          });
        });
        return Object.assign({}, state, {pluginToVersionMap});
    }
    case leftpanelactions.RESET:
      return getInitialState().plugins;

    default:
      return state;
  }
};
var extensions = (state = getInitialState().extensions, action = {}) => {
  switch (action.type) {
    case leftpanelactions.EXTENSIONS_FETCH: {
      const uiSupportedExtension = (extension) => {
        const pipelineType = action.payload.pipelineType;
        const extensionMap = _GLOBALS.pluginTypes[pipelineType];
        return Object.keys(extensionMap).filter(ext => extensionMap[ext] === extension).length;
      };
      return [
        ...state,
        ...action.payload.extensions.filter(uiSupportedExtension)
      ];
    }
    case leftpanelactions.FETCH_ALL_PLUGINS:
      return [
        ...state,
        ...action.payload.extensions
      ];
    case leftpanelactions.RESET:
        return getInitialState().extensions;
    default:
      return state;
  }
};

var LeftPanelStore = (LEFTPANELSTORE_ACTIONS, Redux, ReduxThunk, GLOBALS, DAGPlusPlusFactory, myHelpers, $filter, HydratorPlusPlusNodeService) => {
  leftpanelactions = LEFTPANELSTORE_ACTIONS;
  _GLOBALS = GLOBALS;
  _myHelpers = myHelpers;
  _DAGPlusPlusFactory = DAGPlusPlusFactory;
  _filter = $filter;
  _hydratorNodeService = HydratorPlusPlusNodeService;
  let {combineReducers, applyMiddleware} = Redux;

  let combineReducer = combineReducers({
    plugins,
    extensions
  });

  return Redux.createStore(
    combineReducer,
    getInitialState(),
    window.CaskCommon.CDAPHelpers.composeEnhancers('LeftPanelStore')(
      applyMiddleware(ReduxThunk.default)
    )
  );
};
LeftPanelStore.$inject = ['LEFTPANELSTORE_ACTIONS', 'Redux', 'ReduxThunk', 'GLOBALS', 'DAGPlusPlusFactory', 'myHelpers', '$filter', 'HydratorPlusPlusNodeService'];

angular.module(`${PKG.name}.feature.hydrator`)
  .constant('LEFTPANELSTORE_ACTIONS', {
    'PLUGINS_FETCH': 'PLUGINS_FETCH',
    'FETCH_ALL_PLUGINS': 'FETCH_ALL_PLUGINS',
    'PLUGIN_TEMPLATE_FETCH': 'PLUGIN_TEMPLATE_FETCH',
    'PLUGINS_DEFAULT_VERSION_FETCH': 'PLUGINS_DEFAULT_VERSION_FETCH',

    'EXTENSIONS_FETCH': 'EXTENSIONS_FETCH',

    'RESET': 'LEFTPANELSTORE_RESET',
    'PLUGINS_DEFAULT_VERSION_UPDATE': 'PLUGINS_DEFAULT_VERSION_UPDATE',
    'PLUGIN_DEFAULT_VERSION_CHECK_AND_UPDATE': 'PLUGIN_DEFAULT_VERSION_CHECK_AND_UPDATE'
  })
  .factory('HydratorPlusPlusLeftPanelStore', LeftPanelStore);
