angular.module(PKG.name + '.services')
  .service('EventPipe', function() {
    var events = {};

    this.on = function(event, cb) {
      if (!events[event]) {
        events[event] = [cb];
      } else {
        events[event].push(cb);
      }
    };

    this.emit =  function(event) {
      var args = Array.prototype.slice.call(arguments, 1);
      for (var i = 0; i < events[event].length; i++) {
        events[event][i].apply(this, args);
      }
    };

    this.cancelEvent = function(event) {
      if (events[event]) {
        delete events[event];
      }
    };



  });
