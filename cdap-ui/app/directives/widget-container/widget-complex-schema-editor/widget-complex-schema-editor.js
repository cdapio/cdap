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

function ComplexSchemaEditorController($scope, EventPipe, $timeout, myAlertOnValium, avsc, myHelpers, IMPLICIT_SCHEMA, HydratorPlusPlusNodeService) {
  'ngInject';

  let vm = this;
  let schemaExportTimeout;
  let clearDOMTimeoutTick1;
  let clearDOMTimeoutTick2;

  vm.currentIndex = 0;
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
    vm.schemas = IMPLICIT_SCHEMA[format];
    reRenderComplexSchema();
  }


  vm.formatOutput = (updateDefault = false) => {
    if (!Array.isArray(vm.schemas)) {
      let schema = vm.schemas.schema;
      if (!schema) {
        schema = '';
      }

      vm.schemas = [HydratorPlusPlusNodeService.getOutputSchemaObj(schema)];
    }
    let newOutputSchemas = vm.schemas.map(schema => {
      if (typeof schema.schema !== 'string') {
        schema.schema = JSON.stringify(schema.schema);
      }
      return schema;
    });
    if (vm.onChange && typeof vm.onChange === 'function') {
      vm.onChange({newOutputSchemas});
    }
    if (vm.updateDefaultOutputSchema && updateDefault) {
      vm.updateDefaultOutputSchema({outputSchema: vm.schemas[0].schema});
    }
  };

  function exportSchema() {
    if (vm.url) {
      URL.revokeObjectURL(vm.url);
    }

    if (!vm.schemas) {
      vm.error = 'Cannot export empty schema';
      return;
    }

    vm.schemas = vm.schemas.map(schema => {
      try {
        schema.schema = JSON.parse(schema.schema);
      } catch (e) {
        console.log('ERROR: ', e);
        schema.schema = {
          fields: []
        };
      }
      return schema;
    });

    if (vm.pluginName === 'Stream') {
      vm.schemas[0].fields = vm.schemaPrefix.fields.concat(vm.schemas[0].fields);
    }

    var blob = new Blob([JSON.stringify(vm.schemas, null, 4)], { type: 'application/json'});
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

  let datasetSelectedEvtListner = EventPipe.on('dataset.selected', function (schema, format, isDisabled, datasetId) {
    if (watchProperty && format) {
      vm.pluginProperties[watchProperty] = format;
    }
    // This angular lodash doesn't seem to have isNil
    // have to do this instead of just checking if (isDisabled) because isDisabled might be false
    if (!_.isUndefined(isDisabled) && !_.isNull(isDisabled)) {
      vm.isDisabled = isDisabled;
    }

    if (datasetId) {
      vm.derivedDatasetId = datasetId;
    }

    if (!_.isEmpty(schema) || (_.isEmpty(schema) && vm.isDisabled)) {
      vm.schemas[0].schema = schema;
      vm.formatOutput();
    } else {
      // if dataset name is changed to a non-existing dataset, the schemaObj will be empty,
      // so assign to it the value of the input schema
      if (vm.isDisabled === false && vm.inputSchema) {
        if (vm.inputSchema.length > 0 && vm.inputSchema[0].schema) {
          vm.schemas[0].schema = angular.copy(vm.inputSchema[0].schema);
        } else {
          vm.schemas[0].schema = '';
        }
      }
    }
    reRenderComplexSchema();
  });

  EventPipe.on('schema.export', exportSchema);
  EventPipe.on('schema.clear', () => {
    vm.schemas = [HydratorPlusPlusNodeService.getOutputSchemaObj('')];
    reRenderComplexSchema();
  });

  EventPipe.on('schema.import', (schemas) => {
    vm.clearDOM = true;
    vm.error = '';
    if (typeof schemas === 'string') {
      schemas = JSON.parse(schemas);
    }

    if (!Array.isArray(schemas)) {
      schemas = [HydratorPlusPlusNodeService.getOutputSchemaObj(schemas)];
    // this is for converting old schemas (pre 4.3.2) to new format
    } else if (Array.isArray(schemas) && schemas.length && !schemas[0].hasOwnProperty('schema')) {
      schemas = [HydratorPlusPlusNodeService.getOutputSchemaObj(HydratorPlusPlusNodeService.getSchemaObj(schemas))];
    }

    vm.schemas = schemas.map((schema) => {
      let jsonSchema = schema.schema;

      try {
        if (typeof jsonSchema === 'string') {
          jsonSchema = JSON.parse(jsonSchema);
        }

        if (Array.isArray(jsonSchema)) {
          let recordTypeSchema = {
            name: 'etlSchemaBody',
            type: 'record',
            fields: jsonSchema
          };

          jsonSchema = recordTypeSchema;
        } else if (jsonSchema.type !== 'record') {
          myAlertOnValium.show({
            type: 'danger',
            content: 'Imported schema is not a valid Avro schema'
          });
          vm.clearDOM = false;
          return;
        }

        if (vm.pluginName === 'Stream') {
          modifyStreamSchema(jsonSchema);
        }

        schema.schema = avsc.parse(jsonSchema, { wrapUnions: true });
        return schema;

      } catch (e) {
        vm.error = 'Imported schema is not a valid Avro schema: ' + e;
        vm.clearDOM = false;
        return schema;
      }
    });

    reRenderComplexSchema();
  });

  $scope.$on('$destroy', () => {
    datasetSelectedEvtListner();
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
        schemas: '=',
        inputSchema: '=?',
        isDisabled: '=',
        pluginProperties: '=?',
        config: '=?',
        pluginName: '=',
        updateDefaultOutputSchema: '&',
        onChange: '&',
        isInStudio: '='
      },
      controller: ComplexSchemaEditorController,
      controllerAs: 'SchemaEditor'
    };
  });
