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
          placeholder: '{{myconfig.properties.default || ""}}'
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
          'data-delimiter': '{{ myconfig.properties.delimiter }}',
          'data-type': 'csv'
        }
      },
      'dsv': {
        element: '<my-dsv></my-dsv>',
        attributes: {
          'ng-model': 'model',
          'data-delimiter': '{{ myconfig.properties.delimiter }}',
          'data-type': 'dsv'
        }
      },
      'json-editor': {
        element: '<my-json-textbox></my-json-textbox>',
        attributes: {
          'ng-model': 'model',
          placeholder: 'myconfig.properties.default'
        }
      },
      'javascript-editor': {
        element: '<div my-ace-editor></div>',
        attributes: {
          'ng-model': 'model',
          'config': 'myconfig',
          placeholder: '{{myconfig.properties.default || ""}}'
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
          'ng-options': 'item as item for item in myconfig.properties.values',
          'ng-init': 'model = model.length ? model : myconfig.properties.default'
        }
      },
      'stream-properties': {
        element: '<my-stream-properties></my-stream-properties>',
        attributes: {
          'ng-model': 'model',
          'data-plugins': 'properties',
          'data-config': 'myconfig'
        }
      }
    };
    this.registry['__default__'] = this.registry['textbox'];
  });
