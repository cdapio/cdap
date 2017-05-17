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

class MyPipelineConfigurationsCtrl {
  constructor(uuid, HydratorPlusPlusHydratorService) {
    this.uuid = uuid;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;

    this.actionLabel = 'Run';

    if (this.pipelineAction && this.pipelineAction === 'Scheduling') {
      this.actionLabel = 'Schedule';
    }

    this.checkForReset = (runtimeArguments) => {
      let runtimeArgumentsPairs = runtimeArguments.pairs;
      this.resettingFields = false;
      for (let i = 0; i < runtimeArgumentsPairs.length; i++) {
        if (runtimeArgumentsPairs[i].notDeletable) {
          let runtimeArgKey = runtimeArgumentsPairs[i].key;
          if (this.resolvedMacros.hasOwnProperty(runtimeArgKey)) {
            if (this.resolvedMacros[runtimeArgKey] !== runtimeArgumentsPairs[i].value) {
              runtimeArgumentsPairs[i].showReset = true;
            } else {
              runtimeArgumentsPairs[i].showReset = false;
            }
          }
        }
      }
      return runtimeArguments;
    };

    this.runtimeArguments = this.checkForReset(this.runtimeArguments);

    this.getResettedRuntimeArgument = (index) => {
      let runtimeArgKey = this.runtimeArguments.pairs[index].key;
      this.resettingFields = true;
      this.runtimeArguments.pairs[index].value = this.resolvedMacros[runtimeArgKey];
    };

    this.onRuntimeArgumentsChange = (newRuntimeArguments) => {
      this.runtimeArguments = this.checkForReset(newRuntimeArguments);
    };

    this.applyAndRunPipeline = () => {
      this.applyRuntimeArguments();
      this.runPipeline();
    };

    this.buttonsAreDisabled = () => {
      if (this.runtimeArguments.pairs) {
        return this.runtimeArguments.pairs.some((runtimeArgPair) => {
          let emptyKeyField = (runtimeArgPair.key.length === 0);
          let emptyValueField = (runtimeArgPair.value.length === 0);
          // buttons are disabled when either the key or the value of a pair is empty, but not both
          return (emptyKeyField && !emptyValueField) || (!emptyKeyField && emptyValueField);
        });
      }
      return false;
    };
  }
}

MyPipelineConfigurationsCtrl.$inject = ['uuid', 'HydratorPlusPlusHydratorService'];
  angular.module(PKG.name + '.commons')
  .directive('keyValuePairs', function(reactDirective) {
    return reactDirective(window.CaskCommon.KeyValuePairs);
  })
  .controller('MyPipelineConfigurationsCtrl', MyPipelineConfigurationsCtrl)
  .directive('myPipelineConfigurations', function() {
    return {
      restrict: 'E',
      scope: {
        runtimeArguments: '=',
        resolvedMacros: '=',
        applyRuntimeArguments: '&',
        convertRuntimeArgsToMacros: '&',
        pipelineName: '@',
        runPipeline: '&',
        onClose: '&',
        namespace: '@namespaceId'
      },
      bindToController: true,
      controller: 'MyPipelineConfigurationsCtrl',
      controllerAs: 'PipelineConfigCtrl',
      templateUrl: 'my-pipeline-configurations/my-pipeline-configurations.html'
    };
  });
