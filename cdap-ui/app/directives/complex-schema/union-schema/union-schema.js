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

function UnionSchemaController (avsc, SCHEMA_TYPES, SchemaHelper) {
  'ngInject';

  var vm = this;
  vm.SCHEMA_TYPES = SCHEMA_TYPES.types;

  vm.types = [];

  function init(strJson) {
    let parsed = avsc.parse(strJson, { wrapUnions: true });

    vm.types = parsed.getTypes().map(SchemaHelper.parseType);

    console.log('UNION_TYPES', vm.types);
  }

  init(vm.model);
}

angular.module(PKG.name+'.commons')
.directive('myUnionSchema', function () {
  return {
    restrict: 'E',
    templateUrl: 'complex-schema/union-schema/union-schema.html',
    controller: UnionSchemaController,
    controllerAs: 'UnionSchema',
    bindToController: true,
    scope: {
      model: '=ngModel',
    }
  };
});
