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

function MapSchemaController (avsc, SCHEMA_TYPES, SchemaHelper, $scope) {
  'ngInject';

  var vm = this;

  vm.SCHEMA_TYPES = SCHEMA_TYPES.types;

  vm.fields = {
    keys: null,
    values: null
  };

  function init(strJson) {
    if (!strJson || strJson === 'map') {
      vm.fields.keys = {
        type: 'string',
        displayType: 'string',
        nullable: false
      };

      vm.fields.values = {
        type: 'string',
        displayType: 'string',
        nullable: false
      };
      formatOutput();
      return;
    }

    let parsed = avsc.parse(strJson, { wrapUnions: true });

    vm.fields.keys = SchemaHelper.parseType(parsed.getKeysType());
    vm.fields.values = SchemaHelper.parseType(parsed.getValuesType());

    formatOutput();
  }

  function formatOutput() {
    let obj = {
      type: 'map',
      keys: vm.fields.keys.nullable ? [vm.fields.keys.type, 'null'] : vm.fields.keys.type,
      values: vm.fields.values.nullable ? [vm.fields.values.type, 'null'] : vm.fields.values.type
    };

    vm.model = obj;
  }

  $scope.$watch('MapSchema.fields', formatOutput, true);
  init(vm.model);
}

angular.module(PKG.name+'.commons')
.directive('myMapSchema', function () {
  return {
    restrict: 'E',
    templateUrl: 'complex-schema/map-schema/map-schema.html',
    controller: MapSchemaController,
    controllerAs: 'MapSchema',
    bindToController: true,
    scope: {
      model: '=ngModel'
    }
  };
})
.directive('myMapSchemaWrapper', function ($compile) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      model: '=ngModel',
      type: '@'
    },
    link: (scope, element) => {
      if (scope.type === 'COMPLEX') {
        $compile('<my-map-schema ng-model="model"></my-map-schema')(scope, (cloned) => {
          element.append(cloned);
        });
      }
    }
  };
});
