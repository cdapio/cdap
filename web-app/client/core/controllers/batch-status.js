/*
 * Batch Controller
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
     * Lifecycle
     */

    start: function (appId, id, version, config) {

      var self = this;
      var model = this.get('model');

      model.set('currentState', 'STARTING');

      this.HTTP.post('rest', 'apps', appId, 'mapreduces', id, 'start', function (response) {

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

      this.HTTP.post('rest', 'apps', app.get('id'), 'mapreduces', id, 'stop', function (response) {

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
    }

  });

  Controller.reopenClass({
    type: 'BatchStatus',
    kind: 'Controller'
  });

  return Controller;

});