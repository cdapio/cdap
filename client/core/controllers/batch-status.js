/*
 * Batch Controller
 */

define(['../../helpers/plumber'], function (Plumber) {

  var Controller = Em.Controller.extend({
    typesBinding: 'model.types',

    load: function () {

      var self = this;
      self.updateAlerts();
      this.interval = setInterval(function () {
        self.updateStats();
        self.updateMetrics();
        self.updateAlerts();
      }, C.POLLING_INTERVAL);

      /*
       * Give the chart Embeddables 100ms to configure
       * themselves before updating.
       */
      setTimeout(function () {
        self.updateStats();
        self.updateMetrics();
        self.connectEntities();
      }, C.EMBEDDABLE_DELAY);
    },

    updateStats: function () {
      var self = this;

      // Update timeseries data for current batch.
      C.get.apply(C, this.get('model').getUpdateRequest());

    },

    updateMetrics: function() {
      C.HTTP.post.apply(C, this.get('model').getMetricsRequest());
    },

    updateAlerts: function() {
      C.HTTP.get.apply(C, this.get('model').getAlertsRequest());
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

      model.set('currentState', 'STARTING');

      C.get('manager', {
        method: 'start',
        params: [app, id, version, 'FLOW', config]
      }, function (error, response) {

        if (error) {
          C.Modal.show(error.name, error.message);
        } else {
          model.set('lastStarted', new Date().getTime() / 1000);
        }

      });

    },
    stop: function (app, id, version) {

      var self = this;
      var model = this.get('model');

      model.set('currentState', 'STOPPING');

      C.get('manager', {
        method: 'stop',
        params: [app, id, version]
      }, function (error, response) {

        if (error) {
          C.Modal.show(error.name, error.message);
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

      C.Modal.show("Delete Batch",
        "Are you sure you would like to delete this Batch? This action is not reversible.",
        $.proxy(function (event) {

          var batch = this.get('model');

          C.get('metadata', {
            method: 'deleteBatch',
            params: ['Batch', {
              id: batch.id
            }]
          }, function (error, response) {

            if (error) {
              C.Modal.show('Delete Error', error.message);
            } else {
              window.history.go(-1);
            }

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