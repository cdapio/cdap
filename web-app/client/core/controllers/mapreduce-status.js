/*
 * Mapreduce Controller
 */

define(['helpers/plumber'], function (Plumber) {

  var Controller = Em.Controller.extend({
    typesBinding: 'model.types',

    elements: Em.Object.create(),

    load: function () {
      this.clearTriggers(true);
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

    ajaxCompleted: function () {
      return this.get('timeseriesCompleted');
    },

    clearTriggers: function (value) {
      this.set('timeseriesCompleted', value);
    },

    updateStats: function () {
      if (!this.ajaxCompleted()) {
        return;
      }
      this.get('model').updateState(this.HTTP);
      this.clearTriggers(false);
      C.Util.updateTimeSeries([this.get('model')], this.HTTP, this);

      // C.Util.updateTimeSeries([this.get('model')], this.HTTP);
      // C.Util.updateAggregates([this.get('model'),
      //   this.get('input'), this.get('output')], this.HTTP);

    },

    updateMetrics: function() {
      this.get('model').getMetricsRequest(this.HTTP);
    },

    connectEntities: function() {
      Plumber.connect("batch-map", "batch-reduce");
    },

    unload: function () {

      clearInterval(this.interval);

    },

    /**
     * Action handlers from the View
     */
    exec: function () {

      var model = this.get('model');
      var action = model.get('defaultAction');
      if (action && action.toLowerCase() in model) {
        model[action.toLowerCase()](this.HTTP);
      }

    },

    config: function () {

      var self = this;
      var model = this.get('model');

      this.transitionToRoute('MapreduceStatus.Config');

    }

  });

  Controller.reopenClass({
    type: 'MapreduceStatus',
    kind: 'Controller'
  });

  return Controller;

});
