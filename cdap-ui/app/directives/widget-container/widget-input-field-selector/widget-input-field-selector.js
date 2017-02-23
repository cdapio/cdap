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

function FieldSelectorController(myHelpers) {
  'ngInject';

  let vm = this;

  vm.fieldOptions = [];

  function init() {

    if (!vm.inputSchema || vm.inputSchema.length === 0 ) { return; }

    try {
      let schema = JSON.parse(vm.inputSchema[0].schema);

      vm.fieldOptions = schema.fields.map((field) => {
       return {
          name:field.name,
          value:field.name
        };
      });
      let isEnableAllOptionsSet = myHelpers.objectQuery(vm.config, 'widget-attributes', 'enableAllOptions');
      if (!vm.model && isEnableAllOptionsSet) {
        vm.model = vm.config['widget-attributes'].allOptionValue || '*';
      }

      if (isEnableAllOptionsSet) {
        vm.fieldOptions.unshift({
          name: vm.config['widget-attributes'].allOptionValue || '*',
          value: vm.config['widget-attributes'].allOptionValue || '*'
        });
      }

    } catch (e) {
      console.log('Error', e);
    }
  }

  init();

}


angular.module(PKG.name + '.commons')
  .directive('myInputFieldSelector', function() {
    return {
      restrict: 'E',
      templateUrl: 'widget-container/widget-input-field-selector/widget-input-field-selector.html',
      bindToController: true,
      scope: {
        model: '=ngModel',
        inputSchema: '=',
        config: '='
      },
      controller: FieldSelectorController,
      controllerAs: 'FieldSelector'
    };
  });
