/*
 * Loading screen Controller
 */

define([], function () {

    var Controller = Em.Controller.extend({

      loadingSrc: 'assets/img/services-loading-darkBg.gif',
      completeSrc: 'assets/img/ok-icon.png',

      load: function () {
        var self = this;

        this.set('appfabricImg', this.loadingSrc);
        this.set('streamsImg', this.loadingSrc);
        this.set('transactionsImg', this.loadingSrc);
        this.set('metricsImg', this.loadingSrc);

        /**
         * Check if all services have been loaded periodically and transition page
         * if everything is loaded.
         */
        this.interval = setInterval(function() {
          self.HTTP.rest('system/services/status', function (statuses) {
            self.set('statuses', statuses);
            if (!('APPFABRIC' in statuses
              && 'STREAMS' in statuses
              && 'TRANSACTION' in statuses
              && 'METRICS' in statuses)) {
              $(".warning-text").text('Could not get core Reactor services. Please restart Reactor.');
              return;
            }

            var serviceStatuses = [];
            for (item in statuses) {
              if (statuses.hasOwnProperty(item)) {
                var imgSrc = statuses[item] === 'OK' ? self.completeSrc : self.loadingSrc;
                serviceStatuses.push({
                  name: item,
                  status: statuses[item],
                  imgSrc: imgSrc
                });
              }
            }

            self.set('serviceStatuses', serviceStatuses);



            if (C.Util.isLoadingComplete(statuses)) {
              setTimeout(function() {
                clearInterval(this.interval);
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
