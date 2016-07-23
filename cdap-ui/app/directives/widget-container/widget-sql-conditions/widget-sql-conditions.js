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

function SqlConditionsController() {
  'ngInject';

  let vm = this;

  vm.rules = [];

  vm.mapInputSchema = {};
  vm.stageList = [];

  vm.formatOutput = () => {
    let outputArr = [];

    angular.forEach(vm.rules, (rule) => {
      if (!rule.left.stageName ||
          !rule.left.fieldName ||
          !rule.right.stageName ||
          !rule.right.fieldName) {
        return;
      }

      let output = '';

      output += rule.left.stageName + '.' + rule.left.fieldName;

      if (rule.isEqual) {
        output += ' = ';
      } else {
        output += ' != ';
      }

      output += rule.right.stageName + '.' + rule.right.fieldName;

      outputArr.push(output);
    });

    vm.model = outputArr.join(' & ');
  };

  vm.addRule = () => {
    if (vm.stageList.length === 0) { return; }
    vm.rules.push({
      left: {
        stageName: vm.stageList[0],
        fieldName: vm.mapInputSchema[vm.stageList[0]][0]
      },
      right: {
        stageName: vm.stageList[0],
        fieldName: vm.mapInputSchema[vm.stageList[0]][0]
      },
      isEqual: true
    });
    vm.formatOutput();
  };

  vm.deleteRule = (index) => {
    vm.rules.splice(index, 1);
    vm.formatOutput();
  };

  function initializeOptions() {
    angular.forEach(vm.inputSchema, (input) => {
      vm.stageList.push(input.name);

      vm.mapInputSchema[input.name] = JSON.parse(input.schema).fields.map((field) => {
        return field.name;
      });
    });
  }

  function init() {
    initializeOptions();

    if (!vm.model) {
      vm.addRule();
      return;
    }

    let modelSplit = vm.model.split('&').map((rule) => {
      return rule.trim();
    });

    angular.forEach(modelSplit, (rule) => {
      let ruleSplit = rule.split('=').map((field) => {
        return field.trim().split('.');
      });

      let ruleObj = {
        left: {
          stageName: ruleSplit[0][0],
          fieldName: ruleSplit[0][1]
        },
        right: {
          stageName: ruleSplit[1][0],
          fieldName: ruleSplit[1][1]
        },
        isEqual: true
      };

      vm.rules.push(ruleObj);
    });

  }

  init();
}

angular.module(PKG.name + '.commons')
  .directive('mySqlConditions', function() {
    return {
      restrict: 'E',
      templateUrl: 'widget-container/widget-sql-conditions/widget-sql-conditions.html',
      bindToController: true,
      scope: {
        model: '=ngModel',
        inputSchema: '=',
        disabled: '='
      },
      controller: SqlConditionsController,
      controllerAs: 'SqlConditions'
    };
  });
