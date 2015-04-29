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
          'ng-model': 'model',
          placeholder: 'myconfig.description'
        }
      },
      'password': {
        element: '<input/>',
        attributes: {
          'class': 'form-control',
          'ng-model': 'model',
          type: 'password',
          placeholder: 'myconfig.description'
        }
      },
      'datetime': {
        element: '<my-timestamp-picker></my-timestamp-picker>',
        attributes: {
          'ng-model': 'model',
          'data-label': 'Date'
        }
      },
      'json-editor': {
        element: '<textarea></textarea>',
        attributes: {
          'cask-json-edit': 'model',
          'class': 'form-control',
          placeholder: 'myconfig.description'
        }
      },
      'javascript-editor': {
        element: '<div my-ace-editor></div>',
        attributes: {
          'ng-model': 'model'
        }
      }
    };
    this.registry['__default__'] = this.registry['textbox'];
  });
