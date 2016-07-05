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

function EnumSchemaController (avsc, $timeout, $scope) {
  'ngInject';

  var vm = this;

  vm.symbols = [];
  let timeout;

  vm.addSymbol = (index) => {
    let placement = index === undefined ? 0 : index + 1;
    vm.symbols.splice(placement, 0, {name: ''});
  };

  vm.removeSymbol = (index) => {
    vm.symbols.splice(index, 1);
    if (vm.symbols.length === 0) {
      vm.addSymbol();
    }
    vm.formatOutput();
  };

  vm.formatOutput = () => {
    let symbols = vm.symbols.filter( (symbol) => {
      return symbol.name ? true : false;
    }).map( (symbol) => {
      return symbol.name;
    });

    if (symbols.length === 0) {
      vm.model = '';
      return;
    }

    let obj = {
      type: 'enum',
      symbols: symbols
    };
    vm.model = obj;

    if (typeof vm.parentFormatOutput === 'function') {
      timeout = $timeout(vm.formatOutput);
    }
  };

  init(vm.model);

  function init(strJson) {
    if (!strJson || strJson === 'enum') {
      vm.addSymbol();
      vm.formatOutput();
      return;
    }

    let parsed = avsc.parse(strJson, { wrapUnions: true });
    vm.symbols = parsed.getSymbols().map( (symbol) => { return { name: symbol }; });
    vm.formatOutput();
  }

  $scope.$on('$destroy', () => {
    $timeout.cancel(timeout);
  });
}


angular.module(PKG.name+'.commons')
.directive('myEnumSchema', function () {
  return {
    restrict: 'E',
    templateUrl: 'complex-schema/enum-schema/enum-schema.html',
    controller: EnumSchemaController,
    controllerAs: 'EnumSchema',
    bindToController: true,
    scope: {
      model: '=ngModel',
      parentFormatOutput: '&',
      isDisabled: '='
    }
  };
})
.directive('myEnumSchemaWrapper', function ($compile) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      model: '=ngModel',
      type: '@',
      parentFormatOutput: '&',
      isDisabled: '='
    },
    link: (scope, element) => {
      if (scope.type === 'COMPLEX') {
        $compile('<my-enum-schema ng-model="model" parent-format-output="parentFormatOutput()" is-disabled="isDisabled"></my-enum-schema')(scope, (cloned) => {
          element.append(cloned);
        });
      }
    }
  };
});
