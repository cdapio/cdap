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

function EnumSchemaController (avsc) {
  'ngInject';

  var vm = this;

  vm.symbols = [];

  vm.addSymbol = (index) => {

    // need to add index is 0
    let placement = index ? index + 1 : 0;
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
    vm.model = JSON.stringify(obj);
  };

  init(vm.model);

  function init(strJson) {
    if (!strJson) {
      vm.addSymbol();
      return;
    }

    let parsed = avsc.parse(strJson, { wrapUnions: true });

    vm.symbols = parsed.getSymbols().map( (symbol) => { return { name: symbol }; });
    vm.formatOutput();

    console.log('SYMBOLS', vm.symbols);
  }
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
    }
  };
});
