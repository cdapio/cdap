angular.module(PKG.name + '.services')
  .provider('myAlert', function myAlertProvider() {

    var defaults = this.defaults = {
      limit: 3
    };

    this.$get = ['$alert', function($alert) {

      console.log('singleton');
      var queue = [];
      var count = 0;
      options = angular.extend({}, defaults);

      function display(alert) {
        count++;
        var a = $alert({
          title: alert.title || '',
          content: alert.content || '',
          type: alert.type || 'info'
        });

        var hide = a.hide;
        a.hide = function() {
          hide();
          if (queue.length !== 0) {
            display(queue.shift());
          } else {
            executing = false;
          }
          count--;
        }
      }

      function initial() {
        if (count > options.limit-1) {
          return;
        }

        while(queue.length !== 0) {
          var alert = queue.shift();
          display(alert);

        }
      }

      function myAlertFactory(item) {
        console.log('add');
        queue.push(item);
        initial();
      }

      return myAlertFactory;

    }];

  });