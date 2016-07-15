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

function SqlSelectorController() {
  'ngInject';

  let vm = this;

  vm.parsedInputSchemas = [];

  function init() {
    let initialModel = {};

    if (vm.model) {
      let model = vm.model.split(',');

      angular.forEach(model, (entry) => {
        let split = entry.split(' as ');
        let fieldInfo = split[0].split('.');

        if (!initialModel[fieldInfo[0]]) {
          initialModel[fieldInfo[0]] = {};
        }
        initialModel[fieldInfo[0]][fieldInfo[1]] = split[1] ? split[1] : true;
      });
    }

    angular.forEach(vm.inputSchema, (input) => {
      let schema = JSON.parse(input.schema).fields.map((field) => {
        if (initialModel[input.name] && initialModel[input.name][field.name]) {
          field.selected = true;
          field.alias = initialModel[input.name][field.name] === true ? '' : initialModel[input.name][field.name];
        } else {
          field.selected = false;
          field.alias = '';
        }

        return field;
      });

      vm.parsedInputSchemas.push({
        name: input.name,
        schema: schema,
        expanded: false
      });
    });
  }

  init();

  vm.formatOutput = () => {
    let outputArr = [];

    angular.forEach(vm.parsedInputSchemas, (input) => {
      angular.forEach(input.schema, (field) => {
        if (!field.selected) { return; }

        let outputField = input.name + '.' + field.name;

        if (field.alias) {
          outputField += ' as ' + field.alias;
        }

        outputArr.push(outputField);
      });
    });

    vm.model = outputArr.join(',');
  };

  vm.toggleAllFields = (stage, isSelected) => {
    angular.forEach(stage.schema, (field) => {
      field.selected = isSelected;
    });

    vm.formatOutput();
  };
}

angular.module(PKG.name + '.commons')
  .directive('mySqlSelector', function() {
    return {
      restrict: 'E',
      templateUrl: 'widget-container/widget-sql-select-fields/widget-sql-select-fields.html',
      bindToController: true,
      scope: {
        model: '=ngModel',
        inputSchema: '='
      },
      controller: SqlSelectorController,
      controllerAs: 'SqlSelector'
    };
  });
