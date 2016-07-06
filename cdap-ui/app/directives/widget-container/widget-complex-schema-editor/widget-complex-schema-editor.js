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

function ComplexSchemaEditorController($scope, EventPipe, $timeout, myAlertOnValium, avsc) {
  'ngInject';

  let vm = this;
  let schemaExportTimeout;
  let clearDOMTimeoutTick1;
  let clearDOMTimeoutTick2;

  vm.schemaObj = vm.model;

  vm.clearDOM = false;

  vm.formatOutput = () => {
    if (typeof vm.schemaObj !== 'string') {
      vm.model = JSON.stringify(vm.schemaObj);
    } else {
      vm.model = vm.schemaObj;
    }
  };

  function exportSchema() {
    if (vm.url) {
      URL.revokeObjectURL(vm.url);
    }

    var schema = JSON.parse(vm.model);
    schema = schema.fields;

    angular.forEach(schema, function (field) {
      if (field.readonly) {
        delete field.readonly;
      }
    });

    var blob = new Blob([JSON.stringify(schema, null, 4)], { type: 'application/json'});
    vm.url = URL.createObjectURL(blob);
    vm.exportFileName = 'schema';

    $timeout.cancel(schemaExportTimeout);
    schemaExportTimeout = $timeout(function() {
      document.getElementById('schema-export-link').click();
    });
  }

  EventPipe.on('schema.export', exportSchema);
  EventPipe.on('schema.clear', () => {
    vm.clearDOM = true;

    vm.schemaObj = '';
    $timeout.cancel(clearDOMTimeoutTick1);
    $timeout.cancel(clearDOMTimeoutTick2);
    clearDOMTimeoutTick1 = $timeout(() => {
      clearDOMTimeoutTick2 = $timeout(() => {
        vm.clearDOM = false;
      }, 500);
    });

  });

  EventPipe.on('schema.import', (data) => {
    vm.clearDOM = true;

    let jsonSchema;

    try {
      jsonSchema = JSON.parse(data);

      if (Array.isArray(jsonSchema)) {
        let recordTypeSchema = {
          name: 'etlSchemaBody',
          type: 'record',
          fields: jsonSchema
        };

        vm.schemaObj = avsc.parse(recordTypeSchema, { wrapUnions: true });
      } else if (jsonSchema.type === 'record') {
        vm.schemaObj = avsc.parse(jsonSchema, { wrapUnions: true });
      } else {
        myAlertOnValium.show({
          type: 'danger',
          content: 'Imported schema is not a valid Avro schema'
        });
        vm.clearDOM = false;
        return;
      }
    } catch (e) {
      myAlertOnValium.show({
        type: 'danger',
        content: 'Imported schema is not a valid Avro schema: ' + e
      });
      vm.clearDOM = false;
      return;
    }

    $timeout.cancel(clearDOMTimeoutTick1);
    $timeout.cancel(clearDOMTimeoutTick2);
    clearDOMTimeoutTick1 = $timeout(() => {
      clearDOMTimeoutTick2 = $timeout(() => {
        vm.clearDOM = false;
      }, 500);
    });

  });

  $scope.$on('$destroy', () => {
    EventPipe.cancelEvent('schema.export');
    EventPipe.cancelEvent('schema.import');
    EventPipe.cancelEvent('schema.clear');
    URL.revokeObjectURL($scope.url);
    $timeout.cancel(schemaExportTimeout);
    $timeout.cancel(clearDOMTimeoutTick1);
    $timeout.cancel(clearDOMTimeoutTick2);
  });
}

angular.module(PKG.name + '.commons')
  .directive('myComplexSchemaEditor', function() {
    return {
      restrict: 'E',
      templateUrl: 'widget-container/widget-complex-schema-editor/widget-complex-schema-editor.html',
      bindToController: true,
      scope: {
        model: '=ngModel',
        isDisabled: '='
      },
      controller: ComplexSchemaEditorController,
      controllerAs: 'SchemaEditor'
    };
  });
