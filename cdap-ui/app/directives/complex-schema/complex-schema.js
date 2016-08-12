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

function ComplexSchemaController (avsc, SCHEMA_TYPES, $scope, uuid, $timeout, SchemaHelper) {
  'ngInject';
  var vm = this;

  vm.SCHEMA_TYPES = SCHEMA_TYPES.types;

  vm.parsedSchema = [];
  let recordName;
  let timeout;
  let addFieldTimeout;
  vm.emptySchema = false;

  vm.addField = (index) => {
    let placement = index === undefined ? 0 : index + 1;
    let newField = {
      name: '',
      type: 'string',
      displayType: 'string',
      nullable: false,
      id: uuid.v4(),
      nested: false
    };

    vm.parsedSchema.splice(placement, 0, newField);

    vm.formatOutput();

    $timeout.cancel(addFieldTimeout);
    addFieldTimeout = $timeout(() => {
      let elem = document.getElementById(newField.id);
      angular.element(elem)[0].focus();
    });
  };

  vm.removeField = (index) => {
    vm.parsedSchema.splice(index, 1);
    if (vm.parsedSchema.length === 0) {
      vm.addField();
    }

    vm.formatOutput();
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
    const isEmptySchema = (schemaJson) => {
      if (!schemaJson) {
        return true;
      }
      // we need to check if schemaJson has fields or is already returned by avsc parser in which case the fields will be
      // accessed using getFields() function.
      if (angular.isObject(schemaJson) && !(schemaJson.fields || ( schemaJson.getFields && schemaJson.getFields()) || []).length) {
        return true;
      }
      return false;
    };
    if ((!strJson || strJson === 'record') && !vm.isDisabled) {
      vm.addField();
      recordName = vm.recordName || 'a' + uuid.v4().split('-').join('');
      vm.formatOutput();
      return;
    }
    if (isEmptySchema(strJson) && vm.isDisabled) {
      vm.emptySchema = true;
      return;
    }
    let parsed = avsc.parse(strJson, { wrapUnions: true });
    recordName = vm.recordName || parsed._name;

    vm.parsedSchema = parsed.getFields().map((field) => {
      let type = field.getType();

      let partialObj = SchemaHelper.parseType(type);

      return Object.assign({}, partialObj, {
        id: uuid.v4(),
        name: field.getName()
      });

    });

    if (!vm.isDisabled && vm.parsedSchema.length === 0) {
      vm.addField();
      return;
    }

    vm.formatOutput();
  }

  vm.formatOutput = () => {
    vm.error = '';

    let outputFields = vm.parsedSchema.filter((field) => {
      return field.name && field.type ? true : false;
    }).map( (field) => {
      let obj = {
        name: field.name,
        type: field.nullable ? [field.type, 'null'] : field.type
      };
      return obj;
    });

    if (outputFields.length > 0) {
      let obj = {
        type: 'record',
        name: recordName || 'a' + uuid.v4().split('-').join(''),
        fields: outputFields
      };

      // Validate
      try {
        avsc.parse(obj);
      } catch (e) {
        let err = '' + e;
        err = err.split(':');
        vm.error = err[0] + ': ' + err[1];
        return;
      }

      if (!vm.error) {
        vm.model = obj;
      }
    } else {
      vm.model = '';
    }

    if (typeof vm.parentFormatOutput === 'function') {
      timeout = $timeout(vm.parentFormatOutput);
    }
  };

  init(vm.model);

  $scope.$on('$destroy', () => {
    $timeout.cancel(timeout);
    $timeout.cancel(addFieldTimeout);
  });

}

angular.module(PKG.name+'.commons')
.directive('myComplexSchema', function () {
  return {
    restrict: 'E',
    templateUrl: 'complex-schema/complex-schema.html',
    controller: ComplexSchemaController,
    controllerAs: 'ComplexSchema',
    bindToController: true,
    scope: {
      model: '=ngModel',
      recordName: '=',
      hideHeader: '=',
      parentFormatOutput: '&',
      isDisabled: '=',
      schemaPrefix: '='
    }
  };
})
.directive('myRecordSchema', function ($compile) {
  return {
    restrict: 'E',
    replace: true,
    scope: {
      model: '=ngModel',
      recordName: '=',
      parentFormatOutput: '&',
      isDisabled: '=',
      schemaPrefix: '='
    },
    link: (scope, element) => {
      let elemString = `<my-complex-schema
                          ng-model="model"
                          record-name="recordName"
                          hide-header="true"
                          parent-format-output="parentFormatOutput()"
                          is-disabled="isDisabled"
                          schema-prefix="schemaPrefix"
                        </my-complex-schema>`;

      $compile(elemString)(scope, (cloned) => {
        element.append(cloned);
      });
    }
  };
});
