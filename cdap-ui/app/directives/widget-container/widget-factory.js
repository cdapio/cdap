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
        element: '<textarea></textarea>',
        attributes: {
          'class': 'form-control',
          'ng-trim': 'false',
          'ng-model': 'model',
          'rows': '{{myconfig["widget-attributes"].rows}}',
          placeholder: '{{::myconfig["widget-attributes"].placeholder}}'
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
        element: '<my-json-textbox></my-json-textbox>',
        attributes: {
          'ng-model': 'model',
          placeholder: 'myconfig.properties.default || myconfig["widget-attributes"].default'
        }
      },
      'javascript-editor': {
        element: '<div my-ace-editor></div>',
        attributes: {
          'ng-model': 'model',
          'config': 'myconfig',
          'mode': 'javascript',
          'disabled': 'disabled',
          placeholder: '{{::myconfig["widget-attributes"].default}}'
        }
      },
      'python-editor': {
        element: '<div my-ace-editor></div>',
        attributes: {
          'ng-model': 'model',
          'data-config': 'myconfig',
          'mode': 'python',
          'disabled': 'disabled',
          placeholder: '{{::myconfig["widget-attributes"].default}}'
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
      'stream-selector': {
        element: '<my-dataset-selector></my-dataset-selector>',
        attributes: {
          'ng-model': 'model',
          'dataset-type': 'stream',
          'config': 'myconfig'
        }
      },
      'dataset-selector': {
        element: '<my-dataset-selector></my-dataset-selector>',
        attributes: {
          'ng-model': 'model',
          'dataset-type': 'dataset',
          'config': 'myconfig'
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
      }
    };
    this.registry['__default__'] = this.registry['textbox'];
  });
