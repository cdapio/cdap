/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

function UnionSchemaController (avsc, SCHEMA_TYPES, SchemaHelper, $scope, $timeout) {
  'ngInject';

  var vm = this;
  vm.SCHEMA_TYPES = SCHEMA_TYPES.types;
  let timeout;

  vm.types = [];
  let recordCount = 0;

  vm.addType = (index) => {
    let placement = index === undefined ? 0 : index + 1;
    vm.types.splice(placement, 0, {
      type: 'string',
      displayType: 'string',
      nullable: false,
      nested: false
    });
    vm.formatOutput();
  };

  vm.removeType = (index) => {
    let type = vm.types[index];
    if (type.displayType === 'record') {
      recordCount--;
    }
    vm.types.splice(index, 1);
    if (vm.types.length === 0) {
      vm.addType();
    }
    vm.formatOutput();
  };

  vm.changeType = (item, oldDisplayType) => {
    if (SCHEMA_TYPES.simpleTypes.indexOf(item.displayType) !== -1) {
      item.type = item.displayType;
      vm.formatOutput();
    } else {
      item.type = null;
      if (item.displayType === 'record') {
        recordCount++;
        item.index = recordCount;
      }
    }
    if (oldDisplayType === 'record') {
      recordCount--;
      item.index = null;
    }

    item.nested = SchemaHelper.checkComplexType(item.displayType);
  };

  function init(strJson) {
    if (!strJson || strJson === 'union') {
      vm.addType();
      vm.formatOutput();
      return;
    }

    let parsed = avsc.parse(strJson, { wrapUnions: true });

    vm.types = parsed.getTypes().map(SchemaHelper.parseType);
    angular.forEach(vm.types, (type) => {
      if (type.displayType === 'record') {
        recordCount++;
      }
    });

    vm.formatOutput();
  }

  vm.formatOutput = () => {
    vm.error = '';

    let outputArr = vm.types.map((item) => {
      const type = avsc.formatType(item.type);
      return item.nullable ? [type, 'null'] : type;
    });

    if (outputArr.length === 0) {
      vm.model = '';
      return;
    }

    // Validate
    try {
      avsc.parse(outputArr, { wrapUnions: true });
    } catch (e) {
      let err = '' + e;
      err = err.split(':');
      vm.error = err[0] + ': ' + err[1];
      return;
    }

    vm.model = outputArr;

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
.directive('myUnionSchema', function () {
  return {
    restrict: 'E',
    templateUrl: 'complex-schema/union-schema/union-schema.html',
    controller: UnionSchemaController,
    controllerAs: 'UnionSchema',
    bindToController: true,
    scope: {
      model: '=ngModel',
      parentFormatOutput: '&',
      isDisabled: '='
    }
  };
})
.directive('myUnionSchemaWrapper', function ($compile) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      model: '=ngModel',
      parentFormatOutput: '&',
      isDisabled: '='
    },
    link: (scope, element) => {
      let elemString = `<my-union-schema
                          ng-model="model"
                          parent-format-output="parentFormatOutput()"
                          is-disabled="isDisabled">
                        </my-union-schema>`;

      $compile(elemString)(scope, (cloned) => {
        element.append(cloned);
      });
    }
  };
});
