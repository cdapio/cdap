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

function JoinTypesController() {
  'ngInject';

  let vm = this;

  vm.optionsDropdown = ['Inner', 'Outer'];

  vm.changeJoinType = () => {
    if (vm.joinType === 'Inner') {
      angular.forEach(vm.inputs, (input) => {
        input.selected = true;
      });

      vm.formatOutput();
    }
  };

  vm.formatOutput = () => {
    let outputArr = [];

    angular.forEach(vm.inputs, (input) => {
      if (input.selected) {
        outputArr.push(input.name);
      }
    });

    vm.model = outputArr.join(',');
  };

  function init() {
    vm.joinType = 'Outer';
    vm.inputs = [];

    if (!vm.model) {
      // initialize all to selected when model is empty
      angular.forEach(vm.inputSchema, (input) => {
        vm.inputs.push({
          name: input.name,
          selected: false
        });
      });
      vm.formatOutput();
      return;
    }

    let initialModel = vm.model.split(',').map((input) => {
      return input.trim();
    });

    if (initialModel.length === vm.inputSchema.length) {
      vm.joinType = 'Inner';
      angular.forEach(vm.inputSchema, (input) => {
        vm.inputs.push({
          name: input.name,
          selected: true
        });
      });
      vm.formatOutput();
      return;
    }

    angular.forEach(vm.inputSchema, (input) => {
      vm.inputs.push({
        name: input.name,
        selected: initialModel.indexOf(input.name) !== -1 ? true : false
      });
    });
  }

  init();

}

angular.module(PKG.name + '.commons')
  .directive('myJoinTypes', function() {
    return {
      restrict: 'E',
      templateUrl: 'widget-container/widget-join-types/widget-join-types.html',
      bindToController: true,
      scope: {
        model: '=ngModel',
        inputSchema: '='
      },
      controller: JoinTypesController,
      controllerAs: 'JoinTypes'
    };
  });
