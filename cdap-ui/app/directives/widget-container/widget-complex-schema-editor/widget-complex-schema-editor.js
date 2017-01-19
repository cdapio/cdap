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

function ComplexSchemaEditorController($scope, EventPipe, $timeout, myAlertOnValium, avsc, myHelpers, IMPLICIT_SCHEMA) {
  'ngInject';

  let vm = this;
  let schemaExportTimeout;
  let clearDOMTimeoutTick1;
  let clearDOMTimeoutTick2;

  vm.schemaObj = vm.model;
  vm.clearDOM = false;
  vm.implicitSchemaPresent = false;


  let watchProperty = myHelpers.objectQuery(vm.config, 'property-watch') || myHelpers.objectQuery(vm.config, 'widget-attributes', 'property-watch');

  // Special Case for stream, we need a display only schema prefix of ts and headers
  if (vm.pluginName === 'Stream') {
    vm.schemaPrefix = {
      name: 'schemaPrefix',
      type: 'record',
      fields: [
        {
          name: 'ts',
          type: 'long'
        },
        {
          name: 'headers',
          type: {
            type: 'map',
            keys: 'string',
            values: 'string'
          }
        }
      ]
    };
  }

  if (watchProperty) {
    $scope.$watch(function () {
      return vm.pluginProperties[watchProperty];
    }, changeFormat);
  }

  function changeFormat() {
    let format = vm.pluginProperties[watchProperty];

    var availableImplicitSchema = Object.keys(IMPLICIT_SCHEMA);

    if (availableImplicitSchema.indexOf(format) === -1) {
      vm.implicitSchemaPresent = false;
      return;
    }
    vm.clearDOM = true;
    vm.implicitSchemaPresent = true;
    vm.schemaObj = IMPLICIT_SCHEMA[format];
    reRenderComplexSchema();
  }


  vm.formatOutput = () => {
    vm.error = '';
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

    if (!vm.model) {
      vm.error = 'Cannot export empty schema';
      return;
    }

    var schema;
    try {
      schema = JSON.parse(vm.model);
    } catch(e) {
      console.log('ERROR: ', e);
      schema = {
        fields: []
      };
    }
    schema = schema.fields;

    if (vm.pluginName === 'Stream') {
      schema = vm.schemaPrefix.fields.concat(schema);
    }

    var blob = new Blob([JSON.stringify(schema, null, 4)], { type: 'application/json'});
    vm.url = URL.createObjectURL(blob);
    vm.exportFileName = 'schema';

    $timeout.cancel(schemaExportTimeout);
    schemaExportTimeout = $timeout(function() {
      document.getElementById('schema-export-link').click();
    });
  }

  function reRenderComplexSchema() {
    vm.clearDOM = true;
    $timeout.cancel(clearDOMTimeoutTick1);
    $timeout.cancel(clearDOMTimeoutTick2);
    clearDOMTimeoutTick1 = $timeout(() => {
      clearDOMTimeoutTick2 = $timeout(() => {
        vm.clearDOM = false;
      }, 500);
    });
  }

  function modifyStreamSchema(record) {
    if (record.fields && record.fields.length === 0) { return; }

    if (record.fields[0].name === 'ts' && record.fields[1].name === 'headers') {
      record.fields = record.fields.slice(2);
    }
  }

  EventPipe.on('dataset.selected', function (schema, format) {
    if (watchProperty && format) {
      vm.pluginProperties[watchProperty] = format;
    }
    vm.schemaObj = schema;
    reRenderComplexSchema();
  });

  EventPipe.on('schema.export', exportSchema);
  EventPipe.on('schema.clear', () => {
    vm.schemaObj = '';
    reRenderComplexSchema();
  });

  EventPipe.on('schema.import', (data, append) => {
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

        jsonSchema = recordTypeSchema;
      } else if (jsonSchema.type !== 'record'){
        myAlertOnValium.show({
          type: 'danger',
          content: 'Imported schema is not a valid Avro schema'
        });
        vm.clearDOM = false;
        return;
      }

      if (append && vm.schemaObj.fields){
        jsonSchema.fields = _.union(vm.schemaObj.fields, jsonSchema.fields);
      }

      if (vm.pluginName === 'Stream') {
        modifyStreamSchema(jsonSchema);
      }

      vm.schemaObj = avsc.parse(jsonSchema, { wrapUnions: true });

    } catch (e) {
      myAlertOnValium.show({
        type: 'danger',
        content: 'Imported schema is not a valid Avro schema: ' + e
      });
      vm.clearDOM = false;
      return;
    }

    reRenderComplexSchema();
  });

  $scope.$on('$destroy', () => {
    EventPipe.cancelEvent('dataset.selected');
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
        isDisabled: '=',
        pluginProperties: '=?',
        config: '=?',
        pluginName: '='
      },
      controller: ComplexSchemaEditorController,
      controllerAs: 'SchemaEditor'
    };
  });
