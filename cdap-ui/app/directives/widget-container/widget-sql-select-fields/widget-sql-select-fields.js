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

  vm.expandedButton = true;

  vm.parsedInputSchemas = [];
  vm.aliases = {};
  vm.errors = {
    stageCount: {},
    message: 'Please create one or more aliases for duplicate field names.',
    exist: false
  };

  let modelCopy = angular.copy(vm.model);

  vm.toggleAll = (expansion) => {
    angular.forEach(vm.parsedInputSchemas, (stage) => {
      stage.expanded = expansion;
    });
    vm.expandedButton = !vm.expandedButton;
  };

  vm.resetAll = () => {
    vm.parsedInputSchemas = [];
    init(modelCopy);
  };

  function getStageError() {
    angular.forEach(vm.parsedInputSchemas, (input) => {
      angular.forEach(input.schema, (field) => {
        if (vm.aliases[field.alias] > 1) {
          if (!vm.errors.stageCount[input.name]) {
            vm.errors.stageCount[input.name] = 1;
          } else {
            vm.errors.stageCount[input.name]++;
          }
        }
      });
    });
  }


  vm.formatOutput = () => {
    let outputArr = [];

    vm.errors.stageCount = {};
    vm.errors.exist = false;

    vm.aliases = {};
    angular.forEach(vm.parsedInputSchemas, (input) => {
      angular.forEach(input.schema, (field) => {
        if (!field.selected) { return; }

        let outputField = input.name + '.' + field.name;

        if (field.alias) {
          outputField += ' as ' + field.alias;
        }

        if (!vm.aliases[field.alias]) {
          vm.aliases[field.alias] = 1;
        } else {
          vm.aliases[field.alias]++;
          vm.errors.exist = true;
        }

        outputArr.push(outputField);
      });
    });

    getStageError();

    vm.model = outputArr.join(',');
  };

  vm.toggleAllFields = (stage, isSelected) => {
    angular.forEach(stage.schema, (field) => {
      field.selected = isSelected;
    });

    vm.formatOutput();
  };

  function init(inputModel) {
    let initialModel = {};

    if (inputModel) {
      let model = inputModel.split(',');

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
          field.selected = inputModel ? false : true;
          field.alias = field.name;
        }

        return field;
      });

      vm.parsedInputSchemas.push({
        name: input.name,
        schema: schema,
        expanded: false
      });
    });

    vm.formatOutput();
  }

  init(vm.model);

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
