/*
 * Loading screen Controller
 */

define([], function () {

    var Controller = Em.Controller.extend({

      load: function () {
        var self = this;

        self.set('serviceStatuses', []);

        /**
         * Check if all services have been loaded periodically and transition page
         * if everything is loaded.
         */
        this.interval = setInterval(function() {
          self.HTTP.rest('system/services/status', function (statuses) {
            self.set('statuses', statuses);
            if (!statuses) {
              $(".warning-text").text('Could not get core Reactor services. Please restart Reactor.');
              return;
            }

            var serviceStatuses = [];
            if (Object.prototype.toString.call(statuses) === '[object Object]') {
              for (item in statuses) {
                if (statuses.hasOwnProperty(item)) {
                  var imgSrc = statuses[item] === 'OK' ? 'complete' : 'loading';
                  serviceStatuses.push({
                    name: item,
                    status: statuses[item],
                    imgClass: imgSrc
                  });
                }
              }
            }

            self.set('serviceStatuses', serviceStatuses);



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
      type: 'Loading',
      kind: 'Controller'
    });

    return Controller;

});
