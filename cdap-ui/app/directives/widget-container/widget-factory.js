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
          'data-config': 'myconfig'
        }
      },
      'textbox': {
        element: '<input/>',
        attributes: {
          'class': 'form-control',
          'data-ng-trim': 'false',
          'ng-model': 'model',
          placeholder: '{{myconfig.properties.default || myconfig["widget-attributes"].default || ""}}'
        }
      },
      'textarea': {
        element: '<textarea></textarea>',
        attributes: {
          'class': 'form-control',
          'data-ng-trim': 'false',
          'ng-model': 'model',
          'rows': '{{myconfig["widget-attributes"].rows}}',
          placeholder: '{{myconfig.properties.default || myconfig["widget-attributes"].default || ""}}'
        }
      },
      'password': {
        element: '<input/>',
        attributes: {
          'class': 'form-control',
          'data-ng-trim': 'false',
          'ng-model': 'model',
          type: 'password'
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
          'data-delimiter': '{{ myconfig.properties.delimiter || myconfig["widget-attributes"].delimiter }}',
          'data-type': 'csv'
        }
      },
      'dsv': {
        element: '<my-dsv></my-dsv>',
        attributes: {
          'ng-model': 'model',
          'data-delimiter': '{{ myconfig.properties.delimiter || myconfig["widget-attributes"].delimiter }}',
          'data-type': 'dsv'
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
          placeholder: '{{myconfig.properties.default || myconfig["widget-attributes"].default || ""}}'
        }
      },
      'python-editor': {
        element: '<div my-ace-editor></div>',
        attributes: {
          'ng-model': 'model',
          'config': 'myconfig',
          'mode': 'python',
          placeholder: '{{myconfig.properties.default || myconfig["widget-attributes"].default || ""}}'
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
          'data-dataset-type': 'stream'
        }
      },
      'dataset-selector': {
        element: '<my-dataset-selector></my-dataset-selector>',
        attributes: {
          'ng-model': 'model',
          'data-dataset-type': 'dataset'
        }
      }
    };
    this.registry['__default__'] = this.registry['textbox'];
  });
