angular.module(PKG.name + '.filters')
  .filter('camelCaseFilter', function() {
    return function(input) {
      return input && input.charAt(0).toUpperCase() + input.substr(1);
    }
  })
