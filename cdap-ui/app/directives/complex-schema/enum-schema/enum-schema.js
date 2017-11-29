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

function EnumSchemaController (avsc, $timeout, $scope, uuid) {
  'ngInject';

  var vm = this;

  vm.symbols = [];
  let timeout;
  let addSymbolTimeout;

  vm.addSymbol = (index) => {
    let placement = index === undefined ? 0 : index + 1;
    let newSymbol = {
      name: '',
      id: uuid.v4()
    };

    vm.symbols.splice(placement, 0, newSymbol);

    $timeout.cancel(addSymbolTimeout);
    addSymbolTimeout = $timeout(() => {
      let elem = document.getElementById(newSymbol.id);
      angular.element(elem)[0].focus();
    });
  };

  vm.removeSymbol = (index) => {
    vm.symbols.splice(index, 1);
    if (vm.symbols.length === 0) {
      vm.addSymbol();
    }
    vm.formatOutput();
  };

  vm.formatOutput = () => {
    vm.error = '';

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

    // Validate
    try {
      avsc.parse(obj, { wrapUnions: true });
    } catch (e) {
      vm.error = '' + e;
      return;
    }

    vm.model = obj;

    if (typeof vm.parentFormatOutput === 'function') {
      timeout = $timeout(vm.parentFormatOutput);
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
    vm.symbols = parsed.getSymbols().map( (symbol) => {
      return {
        name: symbol,
        id: uuid.v4()
      };
    });
    vm.formatOutput();
  }

  $scope.$on('$destroy', () => {
    $timeout.cancel(timeout);
    $timeout.cancel(addSymbolTimeout);
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
      parentFormatOutput: '&',
      isDisabled: '='
    },
    link: (scope, element) => {
      let elemString = `<my-enum-schema
                          ng-model="model"
                          parent-format-output="parentFormatOutput()"
                          is-disabled="isDisabled">
                        </my-enum-schema>`;

      $compile(elemString)(scope, (cloned) => {
        element.append(cloned);
      });
    }
  };
});
