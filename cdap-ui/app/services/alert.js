angular.module(PKG.name+'.services')
  .factory('myAlert', function(){
    var list = ['test'];

    return {
      list: list,
      isEmpty: function() {
        return list.length === 0;
      },
      add: function(alert) {
        list.push(alert);
      },
      clear: function() {
        list = [];
      }

    };
  });
