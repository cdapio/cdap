angular.module(PKG.name + '.feature.foo')
  .controller('RuleDriverController', function() {
    this.inputFields = [
      {
        name: 'field1',
        type: 'string'
      },
      {
        name: 'field2',
        type: 'number'
      },
      {
        name: 'field3',
        type: 'boolean'
      }
    ];
  });
