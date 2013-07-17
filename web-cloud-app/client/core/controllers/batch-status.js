/*
 * Batch Controller
 */

define(['../../helpers/plumber'], function (Plumber) {

  var Controller = Em.Controller.extend({
    typesBinding: 'model.types',

    load: function () {

      var self = this;
      //self.updateAlerts();

      this.interval = setInterval(function () {
        self.updateStats();
        // self.updateMetrics();
        // self.updateAlerts();
        self.updateMetrics();
      }, C.POLLING_INTERVAL);

      /*
       * Give the chart Embeddables 100ms to configure
       * themselves before updating.
       */
      setTimeout(function () {
        self.updateStats();
        // self.updateMetrics();
        self.connectEntities();
      }, C.EMBEDDABLE_DELAY);
    },

    updateStats: function () {
      var self = this;

      // Update timeseries data for current batch.
      C.get.apply(C, this.get('model').getUpdateRequest(this.HTTP));

    },

    updateMetrics: function() {
      C.HTTP.post.apply(C, this.get('model').getMetricsRequest());
    },

    connectEntities: function() {
      Plumber.connect("batch-start", "batch-map");
      Plumber.connect("batch-map", "batch-reduce");
      Plumber.connect("batch-reduce", "batch-end");
    },

    unload: function () {

      clearInterval(this.interval);

    },

    /**
     * Lifecycle
     */

    start: function (app, id, version, config) {

      var self = this;
      var model = this.get('model');
      var app = this.get('model.application');

      model.set('currentState', 'STARTING');

        app = this.get('model').get('application');

      this.HTTP.rpc('runnable', 'start', [app, id, version, 'FLOW', config],
        function (response) {

          if (response.error) {
            C.Modal.show(response.error.name, response.error.message);
          } else {
            model.set('lastStarted', new Date().getTime() / 1000);
          }

      });

    },
    stop: function (app, id, version) {

      var self = this;
      var model = this.get('model');
      var app = this.get('model.application');

      model.set('currentState', 'STOPPING');

      this.HTTP.rpc('runnable', 'stop', [app, id, version, 'FLOW'],
        function (response) {

          if (response.error) {
            C.Modal.show(response.error.name, response.error.message);
          }

      });

    },

    /**
     * Action handlers from the View
     */

    config: function () {

      var self = this;
      var model = this.get('model');

      this.transitionToRoute('BatchStatus.Config');

    },

    exec: function (action) {

      var control = $(event.target);
      if (event.target.tagName === "SPAN") {
        control = control.parent();
      }

      var id = control.attr('batch-id');
      var app = control.attr('batch-app');
      var action = control.attr('batch-action');

      if (action && action.toLowerCase() in this) {
        this[action.toLowerCase()](app, id, -1);
      }
    },

    "delete": function () {

      var self = this;

      C.Modal.show("Delete Batch",
        "Are you sure you would like to delete this Batch? This action is not reversible.",
        $.proxy(function (event) {

          var batch = this.get('model');

          self.HTTP.rpc('runnable', 'remove', [batch.app, batch.name, batch.version],
            function (response) {

            C.Modal.hide(function () {

              if (response.error) {
                C.Modal.show('Delete Error', response.error.message || 'No reason given. Please check the logs.');
              } else {
                window.history.go(-1);
              }

            });

          });
        }, this));

    }

  });

  Controller.reopenClass({
    type: 'BatchStatus',
    kind: 'Controller'
  });

  return Controller;

});