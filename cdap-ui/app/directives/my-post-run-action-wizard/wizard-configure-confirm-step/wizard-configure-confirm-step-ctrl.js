/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

class WizardConfigureConfirmStepCtrl {
  constructor($state, myPipelineApi, HydratorPlusPlusPluginConfigFactory, GLOBALS) {
    this.$state = $state;
    this.myPipelineApi = myPipelineApi;
    this.HydratorPlusPlusPluginConfigFactory = HydratorPlusPlusPluginConfigFactory;
    this.showLoadingIcon = true;
    this.action.properties = this.action.properties || {};
    this.widgetJson = {};
    this.requiredPropertyError = GLOBALS.en.hydrator.studio.error['GENERIC-MISSING-REQUIRED-FIELDS'];

    this.onChangeHandler = this.onChangeHandler.bind(this);
    this.validating = null;

    if (this.action && !Object.keys(this.action._backendProperties || {}).length) {
      this.pluginFetch(this.action)
        .then( () => this.showLoadingIcon = false);
    } else {
      this.fetchWidgets(this.action);
    }
  }
  // Fetching Backend Properties
  pluginFetch(action) {
    let {name, version, scope} = action.defaultArtifact;
    this.errorInConfig = false;
    let params = {
      namespace: this.$state.params.namespace,
      pipelineType: name,
      version: version,
      scope: scope,
      extensionType: action.type,
      pluginName: action.name
    };
    this.loadingPlugin = true;
    return this.myPipelineApi.fetchPostActionProperties(params)
      .$promise
      .then( (res) => {
        this.action._backendProperties = res[0].properties;
        this.fetchWidgets(action);
      });
  }

  // Fetching Widget JSON for the plugin
  fetchWidgets(action) {
    let {name, version, scope} = action.defaultArtifact;
    let artifact = {
      name,
      version,
      scope,
      key: 'widgets.' + action.name + '-' + action.type
    };
    return this.HydratorPlusPlusPluginConfigFactory
      .fetchWidgetJson(artifact.name, artifact.version, artifact.scope, artifact.key)
      .then( (widgetJson) => {
        this.widgetJson = widgetJson;
        this.loadingPlugin = false;
      }, () => {
        this.loadingPlugin = false;
      });
  }

  onChangeHandler(values = {}) {
    this.action.properties = values;
  }

  addAction(isClose) {
    if (this.validating) {
      return;
    }
    this.validating = true;
    const callback = () => {
      this.validating = false;
    };
    var fn = this.onActionConfigure();
    if ('undefined' !== typeof fn) {
      fn.call(null, (isClose ? null : this.action), callback);
    }
  }
  gotoPreviousStep() {
    var fn = this.onGotoPreviousStep();
    if ('undefined' !== typeof fn) {
      fn.call(null);
    }
  }
  onItemClicked(event, action) {
    event.stopPropagation();
    event.preventDefault();
    this.action = action;
    this.addAction();
  }
}

angular.module(PKG.name + '.commons')
  .controller('WizardConfigureConfirmStepCtrl', WizardConfigureConfirmStepCtrl);
