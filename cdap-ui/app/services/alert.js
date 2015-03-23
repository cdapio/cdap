angular.module(PKG.name+'.services')
  .factory('myAlert', function(){

    return {
      list: [
        {title: 500, content: 'Something went wrong'},
        {title: 404, content: 'Cannot find landing page'}
      ],
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
