angular.module(PKG.name + '.services')
  .service('stateParamsCache', function() {
    this.params = {};
    this.setCache = function(p) {
      this.params = p;
    };
    this.getCache = function() {
      return this.params;
    };
  });
