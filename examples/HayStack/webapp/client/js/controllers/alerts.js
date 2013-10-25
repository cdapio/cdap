/*
 * Alerts Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    logs: Em.ArrayProxy.create({ content: [] }),
    loading: false,

    load: function () {
    
      var self = this;

      this.set('results', Em.ArrayProxy.create({ content: [] }));

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

      var params = [];
      var args = this.get("params");
      for (var k in args) {
         console.log(args[k]);
         params.push(k + "=" + args[k]);
      }
      
      var url = "/alerts?" + params.join('&');
      console.log(url);
      
      $.getJSON(url, function (results) {

        if (results.error) {
          alert(results.error);
          return;
        }

        var i = results.length;
        while (i--) {

          results[i] = SplunkLite.Search.create(results[i]);

        }
        self.results.pushObjects(results);

      });
    },

    unload: function () {

      this.set('results', Em.ArrayProxy.create({ content: [] }));

    }

  });

  Controller.reopenClass({
    type: 'Alerts',
    kind: 'Controller'
  });

  return Controller;

});
