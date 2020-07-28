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
        element: '<number></number>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
      'textbox': {
        element: '<text-box></text-box>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
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
      'csv': {
        element: '<csv-widget></csv-widget>',
        attributes: {
          'value': 'model',
          'widget-props': 'myconfig["widget-attributes"]',
          'on-change': 'onChange',
          'disabled': 'disabled',
        },
      },
      'dsv': {
        element: '<csv-widget></csv-widget>',
        attributes: {
          'value': 'model',
          'widget-props': 'myconfig["widget-attributes"]',
          'on-change': 'onChange',
          'disabled': 'disabled',
        },
      },
      'ds-multiplevalues': {
        element: '<multiple-values-widget></multiple-values-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
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
      // 'schema': {
      //   element: '<my-schema-editor></my-schema-editor>',
      //   attributes: {
      //     'ng-model': 'model',
      //     'data-config': 'myconfig'
      //   }
      // },
      'keyvalue': {
        element: '<key-value-widget></key-value-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
      'keyvalue-encoded': {
        element: '<key-value-encoded-widget></key-value-encoded-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
      'keyvalue-dropdown': {
        element: '<key-value-dropdown-widget></key-value-dropdown-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
      'function-dropdown-with-alias': {
        element: '<function-dropdown-alias-widget></function-dropdown-alias-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
      'function-dropdown-with-options': {
        element: '<function-dropdown-options-widget></function-dropdown-options-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
      // 'schedule': {
      //   element: '<my-schedule></my-schedule>',
      //   attributes: {
      //     'ng-model': 'model',
      //     'data-config': 'myconfig'
      //   }
      // },
      'select': {
        element: '<select-dropdown></select-dropdown>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
      'dataset-selector': {
        element: '<dataset-selector-widget></dataset-selector-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        },
      },
      'sql-select-fields': {
        element: '<sql-selector-widget></sql-selector-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'extra-config': '{ inputSchema: {{ inputSchema }} }',
        }
      },
      'join-types': {
        element: '<join-type-widget></join-type-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'extra-config': '{ inputSchema: {{ inputSchema }} }',
        }
      },
      'sql-conditions': {
        element: '<sql-conditions-widget></sql-conditions-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'extra-config': '{ inputSchema: {{ inputSchema }} }',
        }
      },
      'input-field-selector': {
        element: '<input-field-dropdown></input-field-dropdown>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'extra-config': '{ inputSchema: {{ inputSchema }} }',
        }
      },
      'wrangler-directives': {
        element: '<wrangler-editor></wrangler-editor>',
        attributes: {
          'value': 'model',
          'disabled': 'disabled',
          'data-config': 'myconfig',
          'properties': 'properties',
          'on-change': 'onChange'
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
      // 'textarea-validate': {
      //   element: '<my-textarea-validate></my-textarea-validate>',
      //   attributes: {
      //     'ng-model': 'model',
      //     'config': 'myconfig',
      //     'disabled': 'disabled',
      //     'node': 'node'
      //   }
      // },
      'multi-select': {
        element: '<multi-select></multi-select>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
      'radio-group': {
        element: '<radio-group-widget></radio-group-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
      'toggle': {
        element: '<toggle-switch-widget></toggle-switch-widget>',
        attributes: {
          'value': 'model',
          'on-change': 'onChange',
          'disabled': 'disabled',
          'widget-props': 'myconfig["widget-attributes"]',
        }
      },
    };
    this.registry['__default__'] = this.registry['textbox'];
  });
