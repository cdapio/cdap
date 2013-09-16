/*
 * Flow Status Controller
 */

define([], function () {

  var Controller = Ember.Controller.extend({

    elements: Em.Object.create(),

    load: function () {

      this.clearTriggers(true);
      var model = this.get('model');
      var self = this;
      this.set('elements.Mapreduces', Em.ArrayProxy.create({content: []}));
      for (var i = 0; i < model.actions.length; i++) {
        this.get('elements.Mapreduces.content').pushObject(
          C.Workflow.create(model.mapReduces[model.actions[i].name]));
      }
      console.log(this.get('elements.Mapreduces.content'));



      this.interval = setInterval(function () {
        self.updateStats();
      }, C.POLLING_INTERVAL);

      /*
       * Give the chart Embeddables 100ms to configure
       * themselves before updating.
       */
      setTimeout(function () {
        self.updateStats();
      }, C.EMBEDDABLE_DELAY);

    },

    unload: function () {

      clearInterval(this.interval);

    },
    
    ajaxCompleted: function () {
      return this.get('timeseriesCompleted') && this.get('aggregatesCompleted') &&
        this.get('ratesCompleted');
    },

    clearTriggers: function (value) {
      this.set('timeseriesCompleted', value);
      this.set('aggregatesCompleted', value);
      this.set('ratesCompleted', value);
    },

    updateStats: function () {
      if (!this.ajaxCompleted()) {
        return;
      }
      this.clearTriggers(false);
      this.get('model').updateState(this.HTTP);
      C.Util.updateTimeSeries([this.get('model')], this.HTTP, this);

      var models = this.get('elements.Flowlet.content').concat(
        this.get('elements.Stream.content'));

      C.Util.updateAggregates(models, this.HTTP, this);

      C.Util.updateRates(models, this.HTTP, this);

    },

    /**
     * Lifecycle
     */
    start: function (appId, id, version, config) {

      var self = this;
      var model = this.get('model');

      model.set('currentState', 'STARTING');
      this.HTTP.post('rest', 'apps', appId, 'workflows', id, 'start', function (response) {

          if (response.error) {
            C.Modal.show(response.error.name, response.error.message);
          } else {
            model.set('lastStarted', new Date().getTime() / 1000);
          }

      });

    },

    stop: function (appId, id, version) {

      var self = this;
      var model = this.get('model');

      model.set('currentState', 'STOPPING');

      this.HTTP.post('rest', 'apps', appId, 'workflows', id, 'stop', function (response) {

          if (response.error) {
            C.Modal.show(response.error.name, response.error.message);
          }

      });

    },

    exec: function () {
      var control = $(event.target);
      if (event.target.tagName === "SPAN") {
        control = control.parent();
      }

      var id = control.attr('flow-id');
      var app = control.attr('flow-app');
      var action = control.attr('flow-action');

      if (action && action.toLowerCase() in this) {
        this[action.toLowerCase()](app, id, -1);
      }
    }

  });

  Controller.reopenClass({
    type: 'WorkflowStatus',
    kind: 'Controller'
  });

  return Controller;

});
