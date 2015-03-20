angular.module(PKG.name + '.services')
  .factory('myAlertQueue', function($alert, $timeout) {

    var queue = [];
    var count = 0;

    function display(alert) {
      count++;
      var a = $alert({
        title: alert.title,
        content: alert.content,
        type: alert.type
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
      if (count > 2) {
        return;
      }

      while(queue.length !== 0) {
        var alert = queue.shift();
        display(alert);

      }
    }

    return {
      add: function(item) {
        queue.push(item);
        initial();
      }
    };

  });