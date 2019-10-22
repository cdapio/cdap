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

class MyLinkButtonCtrl {
  constructor($stateParams) {
    let pipelineDetailStoreState = window.CaskCommon.PipelineDetailStore.getState();
    let currentRun = pipelineDetailStoreState.currentRun;
    this.disabled = !currentRun;
    this.entities.forEach(entity => {
      let datasetId = entity.datasetId;
      if (datasetId.indexOf('${') !== -1 && !this.disabled) {
        let runtimeargs;
        try {
          runtimeargs = JSON.parse(currentRun.properties.runtimeArgs);
        } catch(e) {
          console.log('Unable to parse runtime args to resolve macros.');
          this.disabled = true;
          return;
        }
        /**
         * FIXME: This DOES NOT consider cases where we have nested macros.
         * Not clear even if we have nested macros how to resolve them to a single dataset
         * in UI. For now the assumption it will always be ${macroname}.
         */
        let macroName = datasetId.substring(datasetId.lastIndexOf('${') + 2, datasetId.lastIndexOf('}'));
        datasetId = runtimeargs[macroName];
      }

      const stateParams = {
        namespace: $stateParams.namespace,
        entityType: entity.entityType,
        entityId: datasetId,
      };

      entity.url = window.getTrackerUrl({
        stateParams, 
        stateName: 'tracker.detail.entity.summary',
      });
    });
  }
}
MyLinkButtonCtrl.$inject = ['$stateParams'];

angular.module(PKG.name + '.commons')
  .directive('myLinkButton', function() {
    return {
      restrict: 'E',
      scope: {
        entities: '=',
        currentRun: '=',
      },
      bindToController: true,
      controller: MyLinkButtonCtrl,
      controllerAs: 'MyLinkButtonCtrl',
      templateUrl: 'my-link-button/my-link-button.html'
    };
  });
