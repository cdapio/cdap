/*
 * Stream Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    logs: Em.ArrayProxy.create({ content: [] }),
    loading: false,

    load: function () {

      var self = this;

      this.set('trends', Em.ArrayProxy.create({ content: [] }));

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

      $.getJSON('/trend?level=' + level, function (trends) {

        if (trends.error) {
          alert(trends.error);
          return;
        }

        var i = trends.length;
        while (i--) {

          trends[i] = SplunkLite.Trend.create(trends[i]);

        }
        self.trends.pushObjects(trends);

      });

    },

    unload: function () {

      this.set('trends', Em.ArrayProxy.create({ content: [] }));

    }

  });

  Controller.reopenClass({
    type: 'Trend',
    kind: 'Controller'
  });

  return Controller;

});
