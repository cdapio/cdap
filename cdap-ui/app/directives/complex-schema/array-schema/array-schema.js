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

function ArraySchemaController (avsc, SCHEMA_TYPES, $scope) {
  'ngInject';

  var vm = this;
  vm.SCHEMA_TYPES = SCHEMA_TYPES.types;

  vm.items = {};

  vm.formatOutput = () => {
    let obj = {
      type: 'array',
      items: vm.items.nullable ? [vm.items.type, 'null'] : vm.items.type
    };

    vm.model = obj;
  };

  function init(strJson) {
    if (!strJson || strJson === 'array') {
      vm.items = {
        displayType: 'string',
        type: 'string',
        nullable: false
      };
      vm.formatOutput();
      return;
    }

    let parsed = avsc.parse(strJson, { wrapUnions: true });

    let type = parsed.getItemsType();
    let storedType = type;
    let nullable = false;

    if (type.getTypeName() === 'union:wrapped') {
      type = type.getTypes();

      if (type[1].getTypeName() === 'null') {
        storedType = type[0];
        type = type[0].getTypeName();
        nullable = true;
      } else {
        type = 'union';
      }
    } else {
      type = type.getTypeName();
    }

    vm.items = {
      displayType: type,
      type: storedType,
      nullable: nullable
    };

    vm.formatOutput();
  }

  $scope.$watch('ArraySchema.items', vm.formatOutput, true);
  init(vm.model);

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
      model: '=ngModel'
    }
  };
})
.directive('myArraySchemaWrapper', function ($compile) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      model: '=ngModel',
      type: '@'
    },
    link: (scope, element) => {
      if (scope.type === 'COMPLEX') {
        $compile('<my-array-schema ng-model="model"></my-array-schema')(scope, (cloned) => {
          element.append(cloned);
        });
      }
    }
  };
});
