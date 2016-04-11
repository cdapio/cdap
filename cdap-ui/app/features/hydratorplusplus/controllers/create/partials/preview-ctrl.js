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

class HydratorPlusPlusPreviewTabCtrl {
  constructor(HydratorPlusPlusNodeConfigStore, myPipelineApi, $state, $scope) {
    this.HydratorPlusPlusNodeConfigStore = HydratorPlusPlusNodeConfigStore;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;
    this.$scope = $scope;

    this.HydratorPlusPlusNodeConfigStore.registerOnChangeListener(this.setState.bind(this));

    this.setState();
  }

  setState() {
    this.node = this.HydratorPlusPlusNodeConfigStore.getState().node;

    if (!this.node || !this.node.previewData) { return; }

    if (this.node.previewData.input) {
      this.inputSchema = this.node.previewData.input[0].inputSchema.fields;
    }

    if (this.node.previewData.output) {
      this.outputSchema = this.node.previewData.output[0].outputSchema.fields;
    }
  }
}

HydratorPlusPlusPreviewTabCtrl.$inject = ['HydratorPlusPlusNodeConfigStore', 'myPipelineApi', '$state' ,'$scope'];

angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('HydratorPlusPlusPreviewTabCtrl', HydratorPlusPlusPreviewTabCtrl);
