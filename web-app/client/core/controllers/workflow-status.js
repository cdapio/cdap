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

      this.set('elements.Actions', Em.ArrayProxy.create({content: []}));
      for (var i = 0; i < model.actions.length; i++) {
        model.actions[i].state = 'IDLE';
        model.actions[i].isRunning = false;
        model.actions[i].completionPercentage = 50;
        model.actions[i].id = model.actions[i].name.replace(' ', '');
        this.get('elements.Actions.content').push(Em.Object.create(model.actions[i]));      
      }

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

      this.set('elements.Flowlet', Em.Object.create());
      this.set('elements.Stream', Em.Object.create());

      clearInterval(this.interval);

    },

    connectEntities: function() {
      var actions = this.get('elements.Actions.content').map(function (item) {
        return item.id || item.get('id');
      });
      for (var i = 0; i < actions.length; i++) {
        if (i + 1 < actions.length) {
          Plumber.connect(actions[i], actions[i+1]);    
        }
      }
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
      this.HTTP.post('rest', 'apps', appId, 'flows', id, 'start',
        function (response) {

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

      this.HTTP.post('rest', 'apps', appId, 'flows', id, 'stop',
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

      this.transitionToRoute('FlowStatus.Config');

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
    },

    setFlowletLabel: function (label) {

      var paths = {
        'rate': '/process/events/{app}/flows/{flow}/{id}/ins',
        'pending': '/process/events/{app}/flows/{flow}/{id}/pending',
        'aggregate': '/process/events/{app}/flows/{flow}/{id}'
      };
      var kinds = {
        'rate': 'rates',
        'pending': 'aggregates',
        'aggregate': 'aggregates'
      };

      var flowlets = this.get('elements.Flowlet.content');
      var streams = this.get('elements.Stream.content');

      var i = flowlets.length;
      while (i--) {
        flowlets[i].clearMetrics();
        flowlets[i].trackMetric(paths[label], kinds[label], 'events');
      }

      this.set('__currentFlowletLabel', label);

    },

    flowletLabelName: function () {

      return {
        'rate': 'Flowlet Rate',
        'pending': 'Flowlet Pending',
        'aggregate': 'Flowlet Processed'
      }[this.__currentFlowletLabel];

    }.property('__currentFlowletLabel')

  });

  Controller.reopenClass({
    type: 'WorkflowStatus',
    kind: 'Controller'
  });

  return Controller;

});
