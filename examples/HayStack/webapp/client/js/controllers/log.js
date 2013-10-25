/*
 * Stream Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    logs: Em.ArrayProxy.create({ content: [] }),
    loading: false,

    load: function () {

      var self = this;

      this.set('logs', Em.ArrayProxy.create({ content: [] }));

      // Reset window top.
      window.scrollTo(0, 1);

      // Reset navigation dropdown.
      if ($('.nav-collapse').hasClass('in')) {
        $('.nav-collapse').collapse('toggle');
      }

      this.loadMore();

    },

    loadMore: function (subsequent) {

      var self = this;
      var count = 10;

      $.getJSON('/logs', function (logs) {

        if (logs.error) {
          alert(logs.error);
          return;
        }

        var i = logs.length;
        while (i--) {

          logs[i] = SplunkLite.Log.create(logs[i]);

        }
        self.logs.pushObjects(logs);

      });

    },

    unload: function () {

      this.set('logs', Em.ArrayProxy.create({ content: [] }));

    }

  });

  Controller.reopenClass({
    type: 'Log',
    kind: 'Controller'
  });

  return Controller;

});
