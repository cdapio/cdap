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

class HydratorPlusPlusPostActionsCtrl {
  constructor(HydratorPlusPlusConfigStore, HydratorPlusPlusNodeConfigStore, GLOBALS, myHelpers, myPipelineApi, $state) {
    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusNodeConfigStore = HydratorPlusPlusNodeConfigStore;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.myHelpers = myHelpers;
    this.HydratorPlusPlusNodeConfigStore.registerOnChangeListener(this.setState.bind(this));
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;

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
        this.postActionsList = res;
      });

    this.setState();

  }
  setState() {
    this.state = this.state || {};

  }
}

HydratorPlusPlusPostActionsCtrl.$inject = ['HydratorPlusPlusConfigStore', 'HydratorPlusPlusNodeConfigStore', 'GLOBALS', 'myHelpers', 'myPipelineApi', '$state'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('HydratorPlusPlusPostActionsCtrl', HydratorPlusPlusPostActionsCtrl);
