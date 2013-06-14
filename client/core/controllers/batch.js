/*
 * Batch Controller
 */

define([], function () {

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
      var e0 = jsPlumb.addEndpoint("batch-start"),
      e1 = jsPlumb.addEndpoint("batch-map"),
      e2 = jsPlumb.addEndpoint("batch-reduce"),
      e4 = jsPlumb.addEndpoint("batch-map"),
      e5 = jsPlumb.addEndpoint("batch-reduce"),
      e3 = jsPlumb.addEndpoint("batch-end");
      jsPlumb.connect({ source:e0, target:e1 });
      jsPlumb.connect({ source:e4, target:e5 });
      jsPlumb.connect({ source:e2, target:e3 });
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