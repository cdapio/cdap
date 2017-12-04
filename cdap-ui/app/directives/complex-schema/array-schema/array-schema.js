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

function ArraySchemaController (avsc, SCHEMA_TYPES, $timeout, $scope, SchemaHelper) {
  'ngInject';

  var vm = this;
  vm.SCHEMA_TYPES = SCHEMA_TYPES.types;

  vm.items = {};
  let timeout;

  vm.changeType = () => {
    if (SCHEMA_TYPES.simpleTypes.indexOf(vm.items.displayType) !== -1) {
      vm.items.type = vm.items.displayType;
      vm.formatOutput();
    } else {
      vm.items.type = null;
    }

    vm.items.nested = SchemaHelper.checkComplexType(vm.items.displayType);
  };

  vm.formatOutput = () => {
    vm.error = '';

    let obj = {
      type: 'array',
      items: vm.items.nullable ? [vm.items.type, 'null'] : vm.items.type
    };

    // Validate
    try {
      avsc.parse(obj, { wrapUnions: true });
    } catch (e) {
      let err = '' + e;
      err = err.split(':');
      vm.error = err[0] + ': ' + err[1];
      return;
    }

    vm.model = obj;

    if (typeof vm.parentFormatOutput === 'function') {
      timeout = $timeout(vm.parentFormatOutput);
    }
  };

  function init(strJson) {
    if (!strJson || strJson === 'array') {
      vm.items = {
        displayType: 'string',
        type: 'string',
        nullable: false,
        nested: false
      };
      vm.formatOutput();
      return;
    }

    let parsed = avsc.parse(strJson, { wrapUnions: true });
    let type = parsed.getItemsType();

    vm.items = SchemaHelper.parseType(type);

    vm.formatOutput();
  }

  init(vm.model);

  $scope.$on('$destroy', () => {
    $timeout.cancel(timeout);
  });
}

angular.module(PKG.name+'.commons')
.directive('myArraySchema', function () {
  return {
    restrict: 'E',
    templateUrl: 'complex-schema/array-schema/array-schema.html',
    controller: ArraySchemaController,
    controllerAs: 'ArraySchema',
    bindToController: true,
    scope: {
      model: '=ngModel',
      parentFormatOutput: '&',
      isDisabled: '='
    }
  };
})
.directive('myArraySchemaWrapper', function ($compile) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      model: '=ngModel',
      parentFormatOutput: '&',
      isDisabled: '='
    },
    link: (scope, element) => {
      let elemString = `<my-array-schema
                          ng-model="model"
                          parent-format-output="parentFormatOutput()"
                          is-disabled="isDisabled">
                        </my-array-schema>`;

      $compile(elemString)(scope, (cloned) => {
        element.append(cloned);
      });
    }
  };
});
