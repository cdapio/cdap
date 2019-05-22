/*
 * Copyright © 2015 Cask Data, Inc.
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

angular.module(PKG.name + '.commons')
  .service('WidgetFactory', function() {
    this.registry = {
      'number': {
        element: '<my-number-widget></my-number-widget>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig',
          'disabled': 'disabled',
          'is-field-required': 'isFieldRequired'
        }
      },
      'textbox': {
        element: '<input/>',
        attributes: {
          'class': 'form-control',
          'ng-trim': 'false',
          'ng-model': 'model',
          placeholder: '{{ ::myconfig["widget-attributes"].placeholder}}'
        }
      },
      'textarea': {
        element: '<code-editor></code-editor>',
        attributes: {
          'value': 'model',
          'mode': '"plain_text"',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'rows': '{{myconfig["widget-attributes"].rows}}',
        }
      },
      'password': {
        element: '<my-password></my-password>',
        attributes: {
          'ng-model': 'model',
          'ng-trim': 'false'
        }
      },
      'datetime': {
        element: '<my-timestamp-picker></my-timestamp-picker>',
        attributes: {
          'ng-model': 'model',
          'data-label': 'Date'
        }
      },
      'csv': {
        element: '<my-dsv></my-dsv>',
        attributes: {
          'ng-model': 'model',
          'delimiter': '{{::myconfig["widget-attributes"].delimiter}}',
          'type': 'csv',
          'config': 'myconfig'
        }
      },
      'dsv': {
        element: '<my-dsv></my-dsv>',
        attributes: {
          'ng-model': 'model',
          'delimiter': '{{::myconfig["widget-attributes"].delimiter}}',
          'type': 'dsv',
          'config': 'myconfig'
        }
      },
      'ds-multiplevalues': {
        element: '<my-ds-multiple-values></my-ds-multiple-values>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig'
        }
      },
      'json-editor': {
        element: '<json-editor></json-editor>',
        attributes: {
          'value': 'model',
          'mode': '"json"',
          'on-change': 'onChange',
          'disabled': 'disabled',
        }
      },
      'javascript-editor': {
        element: '<code-editor></code-editor>',
        attributes: {
          'value': 'model',
          'mode': '"javascript"',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'rows': 25,
        }
      },
      'python-editor': {
        element: '<code-editor></code-editor>',
        attributes: {
          'value': 'model',
          'mode': '"python"',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'rows': 25,
        }
      },
      'scala-editor': {
        element: '<code-editor></code-editor>',
        attributes: {
          'value': 'model',
          'mode': '"scala"',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'rows': 25,
        }
      },
      'sql-editor': {
        element: '<code-editor></code-editor>',
        attributes: {
          'value': 'model',
          'mode': '"sql"',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'rows': 15,
        }
      },
      'schema': {
        element: '<my-schema-editor></my-schema-editor>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig'
        }
      },
      'keyvalue': {
        element: '<my-key-value></my-key-value>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig'
        }
      },
      'keyvalue-encoded': {
        element: '<my-key-value-encoded></my-key-value-encoded>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig'
        }
      },
      'keyvalue-dropdown': {
        element: '<my-key-value></my-key-value>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig',
          'is-dropdown': 'true'
        }
      },
      'function-dropdown-with-alias': {
        element: '<my-function-dropdown-with-alias></my-function-dropdown-with-alias>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig'
        }
      },
      'schedule': {
        element: '<my-schedule></my-schedule>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig'
        }
      },
      'select': {
        element: '<select></select>',
        attributes: {
          'ng-model': 'model',
          'class': 'form-control',
          'ng-options': 'item as item for item in (myconfig.properties.values || myconfig["widget-attributes"].values)',
          'ng-init': 'model = model.length ? model : (myconfig.properties.default || myconfig["widget-attributes"].default)'
        }
      },
      'dataset-selector': {
        element: '<my-dataset-selector></my-dataset-selector>',
        attributes: {
          'ng-model': 'model',
          'dataset-type': 'dataset',
          'config': 'myconfig',
          'stage-name': 'stageName'
        }
      },
      'sql-select-fields': {
        element: '<my-sql-selector></my-sql-selector>',
        attributes: {
          'ng-model': 'model',
          'input-schema': 'inputSchema'
        }
      },
      'join-types': {
        element: '<my-join-types></my-join-types>',
        attributes: {
          'ng-model': 'model',
          'input-schema': 'inputSchema'
        }
      },
      'sql-conditions': {
        element: '<my-sql-conditions></my-sql-conditions>',
        attributes: {
          'ng-model': 'model',
          'disabled': 'disabled',
          'input-schema': 'inputSchema'
        }
      },
      'input-field-selector': {
        element: '<my-input-field-selector></my-input-field-selector>',
        attributes: {
          'ng-model': 'model',
          'disabled': 'disabled',
          'input-schema': 'inputSchema',
          'config': 'myconfig'
        }
      },
      'wrangler-directives': {
        element: '<my-wrangler-directives></my-wrangler-directives>',
        attributes: {
          'ng-model': 'model',
          'disabled': 'disabled',
          'data-config': 'myconfig',
          'properties': 'properties'
        }
      },
      'rules-engine-editor': {
        element: '<my-rules-engine-editor></my-rules-engine-editor>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig',
          'properties': 'properties'
        }
      },
      'textarea-validate': {
        element: '<my-textarea-validate></my-textarea-validate>',
        attributes: {
          'ng-model': 'model',
          'config': 'myconfig',
          'disabled': 'disabled',
          'node': 'node'
        }
      },
      'multi-select': {
        element: '<my-multi-select-dropdown></my-multi-select-dropdown>',
        attributes: {
          'ng-model': 'model',
          'config': 'myconfig'
        }
      },
      'radio-group': {
        element: '<my-radio-group></my-radio-group>',
        attributes: {
          'ng-model': 'model',
          'config': 'myconfig'
        }
      },
      'toggle': {
        element: '<my-toggle-switch></my-toggle-switch>',
        attributes: {
          'ng-model': 'model',
          'config': 'myconfig',
          'disabled': 'disabled',
        }
      },
    };
    this.registry['__default__'] = this.registry['textbox'];
  });
