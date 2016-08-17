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
    if (vm.stageList.length < 2) {
      vm.model = '';
      return;
    }

    let outputArr = [];

    angular.forEach(vm.rules, (rule) => {
      let ruleCheck = rule.filter((field) => {
        return !field.fieldName;
      });
      if (ruleCheck.length > 0) { return; }

      let ruleArr = rule.map((field) => {
        return field.stageName + '.' + field.fieldName;
      });

      let output = ruleArr.join(' = ');

      outputArr.push(output);
    });

    vm.model = outputArr.join(' & ');
  };

  vm.addRule = () => {
    if (vm.stageList.length === 0) { return; }

    let arr = [];

    angular.forEach(vm.stageList, (stage) => {
      arr.push({
        stageName: stage,
        fieldName: vm.mapInputSchema[stage][0]
      });
    });

    vm.rules.push(arr);
    vm.formatOutput();
  };

  vm.deleteRule = (index) => {
    vm.rules.splice(index, 1);
    vm.formatOutput();
  };

  function initializeOptions() {
    angular.forEach(vm.inputSchema, (input) => {
      vm.stageList.push(input.name);

      try {
        vm.mapInputSchema[input.name] = JSON.parse(input.schema).fields.map((field) => {
          return field.name;
        });
      } catch (e) {
        console.log('ERROR: ', e);
        vm.error = 'Error parsing input schemas.';
        vm.mapInputSchema[input.name] = [];
      }
    });

    if (vm.stageList.length < 2) {
      vm.error = 'Please connect 2 or more stages.';
    }
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
      let rulesArr = [];

      angular.forEach(rule.split('='), (field) => {
        let splitField = field.trim().split('.');

        // Not including rule if stage has been disconnected
        if (vm.stageList.indexOf(splitField[0]) === -1) { return; }

        rulesArr.push({
          stageName: splitField[0],
          fieldName: splitField[1]
        });
      });

      // Missed fields scenario will happen if the user connects more stages into the join node
      // after they have configured join conditions previously
      let missedFields = vm.stageList.filter((stage) => {
        let filteredRule = rulesArr.filter((field) => {
          return field.stageName === stage;
        });
        return filteredRule.length === 0 ? true : false;
      });

      if (missedFields.length > 0) {
        angular.forEach(missedFields, (field) => {
          rulesArr.push({
            stageName: field,
            fieldName: vm.mapInputSchema[field][0]
          });
        });

        vm.warning = 'Input stages have changed since the last time you edit this node\'s configuration. Please verify the condition is still valid.';
      }

      vm.rules.push(rulesArr);
    });

    vm.formatOutput();
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
