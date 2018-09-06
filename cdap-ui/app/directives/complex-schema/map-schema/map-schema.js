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

function MapSchemaController (avsc, SCHEMA_TYPES, SchemaHelper, $scope, $timeout) {
  'ngInject';

  var vm = this;
  let timeout;

  vm.SCHEMA_TYPES = SCHEMA_TYPES.types;

  vm.fields = {
    keys: null,
    values: null
  };

  vm.changeType = (field) => {
    if (SCHEMA_TYPES.simpleTypes.indexOf(field.displayType) !== -1) {
      field.type = field.displayType;
      vm.formatOutput();
    } else {
      field.type = null;
    }

    field.nested = SchemaHelper.checkComplexType(field.displayType);
  };

  function init(strJson) {
    if (!strJson || strJson === 'map') {
      vm.fields.keys = {
        type: 'string',
        displayType: 'string',
        nullable: false,
        nested: false
      };

      vm.fields.values = {
        type: 'string',
        displayType: 'string',
        nullable: false,
        nested: false
      };
      vm.formatOutput();
      return;
    }

    let parsed = avsc.parse(strJson, { wrapUnions: true });

    vm.fields.keys = SchemaHelper.parseType(parsed.getKeysType());
    vm.fields.values = SchemaHelper.parseType(parsed.getValuesType());

    vm.formatOutput();
  }

  vm.formatOutput = () => {
    vm.error = '';

    const keysType = avsc.formatType(vm.fields.keys.type);
    const valuesType = avsc.formatType(vm.fields.values.type);

    let obj = {
      type: 'map',
      keys: vm.fields.keys.nullable ? [keysType, 'null'] : keysType,
      values: vm.fields.values.nullable ? [valuesType, 'null'] : valuesType
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

  init(vm.model);

  $scope.$on('$destroy', () => {
    $timeout.cancel(timeout);
  });
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
      model: '=ngModel',
      parentFormatOutput: '&',
      isDisabled: '='
    }
  };
})
.directive('myMapSchemaWrapper', function ($compile) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      model: '=ngModel',
      parentFormatOutput: '&',
      isDisabled: '='
    },
    link: (scope, element) => {
      let elemString = `<my-map-schema
                          ng-model="model"
                          parent-format-output="parentFormatOutput()"
                          is-disabled="isDisabled">
                        </my-map-schema>`;

      $compile(elemString)(scope, (cloned) => {
        element.append(cloned);
      });
    }
  };
});
