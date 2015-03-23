angular.module(PKG.name+'.services')
  .factory('myAlert', function(){

    return {
      list: [],
      isEmpty: function() {
        return this.list.length === 0;
      },
      add: function(alert) {
        this.list.push(alert);
      },
      clear: function() {
        this.list = [];
      }

    };
  });
