/*
 * Connection error Controller
 */

define([], function () {

    var Controller = Em.Controller.extend({

      load: function () {
        var self = this;

        /**
         * Check if all services have been loaded periodically and transition page
         * if everything is loaded.
         */
        this.interval = setInterval(function() {
          self.HTTP.rest('system/services/status', function (statuses) {

            if (C.Util.isLoadingComplete(statuses)) {
              setTimeout(function() {
                clearInterval(self.interval);
                $("#warning").hide();
                self.transitionToRoute('Overview');
              }, 500);
            }

          });
        }, 1000);
      },

      unload: function () {
        clearInterval(this.interval);
      }

    });

    Controller.reopenClass({
      type: 'ConnectionError',
      kind: 'Controller'
    });

    return Controller;

});
