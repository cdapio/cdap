/*
 * Copyright © 2016 Cask Data, Inc.
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

class HydratorPlusPlusPostActionsCtrl {
  constructor(HydratorPlusPlusConfigStore, myPipelineApi, $state, HydratorPlusPlusPluginConfigFactory, uuid, HydratorPlusPlusConfigActions, myAlertOnValium, GLOBALS, myHelpers) {
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;
    this.HydratorPlusPlusPluginConfigFactory = HydratorPlusPlusPluginConfigFactory;
    this.uuid = uuid;
    this.HydratorPlusPlusConfigActions = HydratorPlusPlusConfigActions;
    this.myAlertOnValium = myAlertOnValium;
    this.GLOBALS = GLOBALS;


    this.requiredPropertyError = this.GLOBALS.en.hydrator.studio.error['GENERIC-MISSING-REQUIRED-FIELDS'];

    this.HydratorPlusPlusConfigStore.registerOnChangeListener(this.setState.bind(this));

    let artifact = this.HydratorPlusPlusConfigStore.getArtifact();
    this.postActionsList = [];

    let params = {
      namespace: this.$state.params.namespace,
      pipelineType: artifact.name,
      version: artifact.version,
      extensionType: 'postaction'
    };
    this.myPipelineApi.fetchPlugins(params)
      .$promise
      .then( (res) => {
        let filteredPlugins = this.filterPlugins(res);

        this.postActionsList = filteredPlugins.map( postaction => {
          // Coverting the name to lowercase before lookup as we can maintain a case insensitive map in case backend wants to change from camelcase or to any other case.
          return Object.assign({}, postaction, { label: myHelpers.objectQuery(GLOBALS.pluginTypes, 'post-run-actions', postaction.name.toLowerCase()) || postaction.name });
        });
      });

    this.setState();

  }
  setState() {
    this.postActions = this.HydratorPlusPlusConfigStore.getPostActions();
    this.activeTab = 'OVERVIEW';
  }

  /*
   * Because there is no ability to change version for post-run actions in UI,
   * this function is intended to show only 1 version. The assumption is that
   * the list the backend sends, the latest version of the plugin is the one last
   * on the list.
   **/
  filterPlugins(results) {
    let pluginsMap = {};
    let resultPlugin = [];

    angular.forEach(results, (plugin) => {
      if (!pluginsMap[plugin.name]) {
        pluginsMap[plugin.name] = [];
      }
      pluginsMap[plugin.name].push(plugin);
    });

    angular.forEach(pluginsMap, (value) => {
      resultPlugin.push(value[value.length - 1]);
    });

    return resultPlugin;
  }

  tabClick(tab) {
    this.activeTab = tab;

    if (tab === 'OVERVIEW') { return; }

    this.isEdit = false;
    this.groupsConfig = null;
    this.pluginConfig = {
      name: tab.name + '-' + this.uuid.v4(),
      plugin: {
        name: tab.name,
        type: tab.type,
        artifact: tab.artifact,
        properties: {}
      }
    };

    this.pluginFetch(tab);
  }

  edit(config) {
    this.isEdit = true;
    let tab = {
      artifact: {
        name: config.plugin.artifact.name,
        version: config.plugin.artifact.version,
        scope: config.plugin.artifact.scope
      },
      name: config.plugin.name,
      type: config.plugin.type
    };

    this.pluginConfig = config;
    this.pluginFetch(tab);

    this.activeTab = tab;
  }

  // Fetching Backend Properties
  pluginFetch(tab) {
    this.errorInConfig = false;
    let params = {
      namespace: this.$state.params.namespace,
      pipelineType: tab.artifact.name,
      version: tab.artifact.version,
      scope: tab.artifact.scope,
      extensionType: tab.type,
      pluginName: tab.name
    };

    this.myPipelineApi.fetchPostActionProperties(params)
      .$promise
      .then( (res) => {
        this.pluginConfig._backendProperties = res[0].properties;
        this.fetchWidgets(tab);
      });
  }

  // Fetching Widget JSON for the plugin
  fetchWidgets(tab) {
    let artifact = {
      name: tab.artifact.name,
      version: tab.artifact.version,
      scope: tab.artifact.scope,
      key: 'widgets.' + tab.name + '-' + tab.type
    };
    this.HydratorPlusPlusPluginConfigFactory
      .fetchWidgetJson(artifact.name, artifact.version, artifact.scope, artifact.key)
      .then( (widgetJson) => {
        this.noConfig = false;

        this.groupsConfig = this.HydratorPlusPlusPluginConfigFactory.generateNodeConfig(this.pluginConfig._backendProperties, widgetJson);

        // Initializing default value
        angular.forEach(this.groupsConfig.groups, (group) => {
          angular.forEach(group.fields, (field) => {
            if (field.defaultValue) {
              this.pluginConfig.properties[field.name] = this.pluginConfig.properties[field.name] || field.defaultValue;
            }
          });
        });
      }, () => {
        this.noConfig = true;
      });
  }

  cancel() {
    this.activeTab = 'OVERVIEW';
    this.pluginConfig = {};
    this.groupsConfig = null;
  }

  fieldValidation() {
    let isValid = true;
    angular.forEach(this.pluginConfig._backendProperties, (value, key) => {
      if (value.required && !this.pluginConfig.plugin.properties[key]) {
        isValid = false;
      }
    });

    return isValid;
  }

  save(isEdit) {
    let isValid = this.fieldValidation();

    if (!isValid) {
      this.errorInConfig = true;
      this.myAlertOnValium.show({
        type: 'danger',
        content: 'Please fill out all required fields.'
      });
      return;
    }

    delete this.pluginConfig._backendProperties;

    if (isEdit) {
      this.HydratorPlusPlusConfigActions.editPostAction(angular.copy(this.pluginConfig));

      this.myAlertOnValium.show({
        type: 'success',
        content: this.pluginConfig.plugin.name + ' post action saved.'
      });
    } else {
      this.HydratorPlusPlusConfigActions.addPostAction(angular.copy(this.pluginConfig));

      this.myAlertOnValium.show({
        type: 'success',
        content: this.pluginConfig.plugin.name + ' post action added.'
      });
    }

    this.pluginConfig = {};
    this.groupsConfig = null;
  }

  deleteAction(config) {
    this.HydratorPlusPlusConfigActions.deletePostAction(config);
  }
}

HydratorPlusPlusPostActionsCtrl.$inject = ['HydratorPlusPlusConfigStore', 'myPipelineApi', '$state', 'HydratorPlusPlusPluginConfigFactory', 'uuid', 'HydratorPlusPlusConfigActions', 'myAlertOnValium', 'GLOBALS', 'myHelpers'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('HydratorPlusPlusPostActionsCtrl', HydratorPlusPlusPostActionsCtrl);
