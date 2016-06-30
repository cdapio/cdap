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

function ArraySchemaController (avsc, SCHEMA_TYPES) {
  'ngInject';

  var vm = this;
  vm.SCHEMA_TYPES = SCHEMA_TYPES.types;

  vm.items = {};

  function init(strJson) {
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

    console.log('ARRAY_ITEMS', vm.items);
  }

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
});
