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

function TextareaValidateController($state, myPipelineApi, myHelpers) {
  let vm = this;

  let methodName = myHelpers.objectQuery(vm.config, 'widget-attributes', 'validate-endpoint');
  vm.placeholder = myHelpers.objectQuery(vm.config, 'widget-attributes', 'placeholder');
  vm.buttonText = myHelpers.objectQuery(vm.config, 'widget-attributes', 'validate-button-text') || 'Validate';
  const successMessage = myHelpers.objectQuery(vm.config, 'widget-attributes', 'validate-success-message') || 'Valid';

  vm.warning = '';
  vm.success = '';

  vm.validate = () => {
    let params = {
      namespace: $state.params.namespace,
      artifactName: vm.node.plugin.artifact.name,
      version: vm.node.plugin.artifact.version,
      pluginType: vm.node.type,
      pluginName: vm.node.plugin.name,
      methodName: methodName,
      scope: vm.node.plugin.artifact.scope
    };

    // May need to be more specific to the api method
    myPipelineApi.postPluginMethod(params, vm.node.plugin.properties)
      .$promise
      .then(() => {
        vm.warning = '';
        vm.success = successMessage;
      }, (err) => {
        vm.warning = err.data || err;
        vm.success = '';
      });
  };
}

angular.module(PKG.name + '.commons')
  .directive('myTextareaValidate', function() {
    return {
      restrict: 'E',
      templateUrl: 'widget-container/widget-textarea-validate/widget-textarea-validate.html',
      bindToController: true,
      scope: {
        model: '=ngModel',
        config: '=',
        node: '='
      },
      controller: TextareaValidateController,
      controllerAs: 'TextareaValidate'
    };
  });
