/*
 * Dataset Controller
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
            if (!('APPFABRIC' in statuses
              && 'STREAMS' in statuses
              && 'TRANSACTION' in statuses
              && 'METRICS' in statuses)) {
              $(".warning-text").text('Could not get Reactor status. Please restart Reactor.');
              return;
            }
            var appfabricImg = statuses.APPFABRIC === 'OK' ? self.completeSrc : self.loadingSrc;
            var streamsImg = statuses.STREAMS === 'OK' ? self.completeSrc : self.loadingSrc;
            var transactionsImg = statuses.TRANSACTION === 'OK' ? self.completeSrc : self.loadingSrc;
            var metricsImg = statuses.METRICS === 'OK' ? self.completeSrc : self.loadingSrc;

            self.set('appfabricImg', appfabricImg);
            self.set('streamsImg', streamsImg);
            self.set('transactionsImg', transactionsImg);
            self.set('metricsImg', metricsImg);

            if (C.Util.isLoadingComplete(statuses)) {
              setTimeout(function() {
                clearInterval(this.interval);
                self.transitionToRoute('Overview');
              }, 500);
            }

          });
        }, 1000);
      },

      unload: function () {}

    });

    Controller.reopenClass({
      type: 'Loading',
      kind: 'Controller'
    });

    return Controller;

});
