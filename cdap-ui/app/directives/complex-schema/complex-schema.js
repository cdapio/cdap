/*
 * Copyright © 2016-2020 Cask Data, Inc.
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
  // lazy loading parameters
  $scope.id = uuid.v4();
  $scope.lazyLoadedParsedSchema = [];
  vm.windowSize = 50;
  vm.DEFAULT_WINDOW_SIZE = 50;
  vm.lazyloading = false;

  $scope.$watch('domLoaded', () => {
    /*
      Wait for the dom to be loaded
      Equivalent of componentDidMount :sigh:
    */
    if (!$scope.domLoaded) {
      return;
    }
    /*
      For some reason if the dom is loaded but none of the fields are
      rendered we don't need to do anything.
    */
    const fields = document.querySelectorAll(`#schema-container-${$scope.id} .field-row`);
    if (!fields.length) {
      return;
    }
    /* jshint ignore:start */
    vm.io = new IntersectionObserver(
      (entries) => {
        let lastVisibleElement = vm.windowSize;
        for (const entry of entries) {
          const id = entry.target.getAttribute('lazyload-id');
          const numID = parseInt(id, 10);
          if (entry.isIntersecting) {
            lastVisibleElement = numID + vm.DEFAULT_WINDOW_SIZE > vm.windowSize ? numID + vm.DEFAULT_WINDOW_SIZE : numID;
          }
        }
        if (lastVisibleElement > vm.windowSize) {
          vm.windowSize = lastVisibleElement;
          vm.lazyloading = true;
          /*
            The timeout is to sort-of give a smooth transition
            scroll => loading ... => then after a second show the fields
            This is the best effort to avoid jankiness while scrolling
          */
          $timeout(() => {
            // This is to trigger a re-render for angular.
            $scope.$apply(function() {
              $scope.lazyLoadedParsedSchema = $scope.lazyLoadedParsedSchema.concat(
                vm.parsedSchema.slice($scope.lazyLoadedParsedSchema.length, lastVisibleElement)
              );
              vm.lazyloading = false;
            });
          }, 1000);
        }
      },
      {
        // We don't have a root. Browser fallsback to document
        threshold: [0, 1],
      }
    );
    /* jshint ignore:end */
    $scope.observeFields();
  });

  $scope.observeFields = () => {
    if (!vm.io) {
      return;
    }
    document.querySelectorAll(`#schema-container-${$scope.id} .field-row`).forEach((entry) => {
      vm.io.observe(entry);
    });
  };

  vm.addField = (index) => {
    let placement = index === undefined ? 0 : index + 1;
    let newField = {
      name: '',
      type: 'string',
      displayType: 'string',
      nullable: false,
      id: uuid.v4(),
      nested: false,
      collapse: false,
    };

    vm.parsedSchema.splice(placement, 0, newField);
    $scope.lazyLoadedParsedSchema.splice(placement, 0, newField);

    vm.formatOutput();

    if (index !== undefined) {
      $timeout.cancel(addFieldTimeout);
      addFieldTimeout = $timeout(() => {
        let elem = document.getElementById(newField.id);
        angular.element(elem)[0].focus();
      });
    }
  };

  vm.removeField = (index) => {
    vm.parsedSchema.splice(index, 1);
    if (vm.parsedSchema.length === 0) {
      vm.addField();
    }
    $scope.lazyLoadedParsedSchema = vm.parsedSchema.slice(0, vm.windowSize);
    vm.formatOutput();
  };

  vm.changeType = (field) => {
    if (SCHEMA_TYPES.simpleTypes.indexOf(field.displayType) !== -1) {
      field.type = field.displayType;
      vm.formatOutput();
    } else {
      field.collapse = false;
      field.type = null;
    }

    field.nested = SchemaHelper.checkComplexType(field.displayType);
  };

  vm.pasteFields = (event, index) => {
    event.preventDefault();
    let data = [];
    let pastedData = event.clipboardData.getData('text/plain');
    let pastedDataArr = pastedData.replace(/[\n\r\t,| ]/g, '$').split('$');
    pastedDataArr.filter((name) => {
      if (name) {
        data.push({
          'name': name,
          'type': 'string',
          displayType: 'string',
          nullable: false,
          id: uuid.v4(),
          nested: false
        });
      }
    });

    document.getElementsByClassName('bottompanel-body')[0].scrollTop = 0;
    // This happens when the user adds a new field, then paste the data.
    // In that case we should delete the empty before pasting.
    if (!vm.parsedSchema[index].name) {
      vm.parsedSchema.splice(index, 1);
    }

    vm.parsedSchema = vm.parsedSchema.concat(data);
    $scope.lazyLoadedParsedSchema = vm.parsedSchema.slice(0, vm.windowSize);
    vm.formatOutput();
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
      let recordNameWithIndex;
      if (vm.isRecordSchema && vm.typeIndex) {
        recordNameWithIndex = 'record' + vm.typeIndex;
      }
      recordName = vm.recordName || recordNameWithIndex || 'a' + uuid.v4().split('-').join('');
      vm.formatOutput();
      return;
    }
    if (isEmptySchema(strJson) && vm.isDisabled) {
      vm.emptySchema = true;
      return;
    }
    // TODO(CDAP-13010): for splitters, the backend returns port names similar to [schemaName].string or [schemaName].int.
    // However, some weird parsing code in the avsc library doesn't allow primitive type names to be after periods(.),
    // so we have to manually make this change here. Ideally the backend should provide a different syntax for port
    // names so that we don't have to do this hack in the UI.

    if (strJson.name) {
      strJson.name = strJson.name.replace('.', '.type');
    }
    let parsed = avsc.parse(strJson, { wrapUnions: true });
    recordName = vm.recordName || parsed._name;

    vm.parsedSchema = parsed.getFields().map((field) => {
      let type = field.getType();
      let partialObj = SchemaHelper.parseType(type);

      return Object.assign({}, partialObj, {
        id: uuid.v4(),
        name: field.getName(),
        collapse: true,
      });

    });

    if (!vm.isDisabled && vm.parsedSchema.length === 0) {
      vm.addField();
      return;
    }

    $scope.lazyLoadedParsedSchema = vm.parsedSchema.slice(0, vm.windowSize);

    vm.formatOutput(true);
  }

  // In some cases, we edit the schema when the user opens a node, so the schema changes without the
  // user doing anything. In those cases we should update the 'default' schema state to the state after
  // we've done our initialzing i.e. updateDefault = true. Defaults to false.
  vm.formatOutput = (updateDefault = false) => {
    vm.error = '';

    let outputFields = vm.parsedSchema.filter((field) => {
      return field.name && field.type ? true : false;
    }).map( (field) => {
      const type = avsc.formatType(field.type);

      let obj = {
        name: field.name,
        type: field.nullable ? [type, 'null'] : type
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
        avsc.parse(obj, { wrapUnions: true });
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
      timeout = $timeout(vm.parentFormatOutput.bind(null, {updateDefault}));
    }
  };

  if (vm.derivedDatasetId) {
    vm.disabledTooltip = `The dataset '${vm.derivedDatasetId}' already exists. Its schema cannot be modified.`;
  }
  if (vm.isInputSchema) {
    vm.disabledTooltip = `This input schema has been derived from the output schema of the previous node(s) and cannot be changed.`;
  }

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
      isRecordSchema: '=',
      typeIndex: '=',
      parentFormatOutput: '&',
      isDisabled: '=',
      schemaPrefix: '=',
      derivedDatasetId: '=',
      isInputSchema: '=',
      isInStudio: '=',
      errors: '=',
    },
    link: (scope, element) => {
      scope.domLoaded = false;
      /*
        This watch is here because when we update the lazyLoadedParsedSchema we need to
        observe the newly added fields. There is no way to check if new fields are
        added to the DOM unless we watch it.
        This cannot be done as soon as we update lazyLoadedParsedSchema in the controller
        because angular doesn't guarantee that the DOM node will be available immediately
        after we add new fields to the array.
      */
      scope.$watch(
        function() {
          return document.querySelectorAll(`#schema-container-${scope.id} .field-row`).length;
        },
        function(newValue, oldValue) {
          if (newValue !== oldValue) {
            scope.observeFields();
          }
        }
      );
      element.ready(() => {
        scope.$apply(() => {
          scope.domLoaded = true;
        });
      });
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
      typeIndex: '=',
      parentFormatOutput: '&',
      isDisabled: '=',
      schemaPrefix: '=',
    },
    link: (scope, element) => {
      let elemString = `<my-complex-schema
                          ng-model="model"
                          record-name="recordName"
                          type-index="typeIndex"
                          is-record-schema="true"
                          parent-format-output="parentFormatOutput()"
                          is-disabled="isDisabled"
                          schema-prefix="schemaPrefix">
                        </my-complex-schema>`;

      $compile(elemString)(scope, (cloned) => {
        element.append(cloned);
      });
    }
  };
});
