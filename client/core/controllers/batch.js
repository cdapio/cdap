/*
 * Batch Controller
 */

define(['../../helpers/plumber'], function (Plumber) {

  var Controller = Em.Controller.extend({
    typesBinding: 'model.types',

    load: function () {

      var self = this;
      this.interval = setInterval(function () {
        self.updateStats();
        self.updateMetrics();
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
      C.get.apply(C, this.get('model').getMetricsRequest());
    },

    connectEntities: function() {
      Plumber.connect("batch-start", "batch-map");
      Plumber.connect("batch-map", "batch-reduce");
      Plumber.connect("batch-reduce", "batch-end");
    },

    unload: function () {

      clearInterval(this.interval);

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