/*
 * Flow Status Controller
 */

define(['helpers/plumber'], function (Plumber) {

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

        model.actions[i].appId = self.get('model').app;
        model.actions[i].divId = model.actions[i].name.replace(' ', '');

        if ('mapReduceName' in model.actions[i].options) {
          var transformedModel = C.Batch.transformModel(model.actions[i]);
          var batchModel = C.Batch.create(transformedModel);
          this.get('elements.Actions.content').push(batchModel);
        } else {
          this.get('elements.Actions.content').push(Em.Object.create(model.actions[i]));
        }

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
        self.connectEntities();
      }, C.EMBEDDABLE_DELAY);

    },

    formatNextRuns: function () {
      this.set('formattedRuns', Em.ArrayProxy.create({content: []}));

      for (var i = 0; i < this.get('model.nextRuns').length; i++) {
        var run = this.get('model.nextRuns')[i];
        this.get('formattedRuns.content').push(new Date(+run.time));
      }

    }.observes('model.nextRuns.@each'),

    unload: function () {

      clearInterval(this.interval);
      this.set('elements.Actions.content', []);

    },

    connectEntities: function() {
      var actions = this.get('elements.Actions.content').map(function (item) {
        return item.divId || item.get('divId');
      });

      for (var i = 0; i < actions.length; i++) {
        if (i + 1 < actions.length) {
          Plumber.connect(actions[i], actions[i+1]);
        }
      }
    },

    ajaxCompleted: function () {
      return this.get('statsCompleted');
    },

    clearTriggers: function (value) {
      this.set('statsCompleted', value);
    },

    updateStats: function () {
      var self = this;
      if (!this.ajaxCompleted()) {
        return;
      }
      this.clearTriggers(false);
      var appId = this.get('model.app'),
        workflowId = this.get('model.name');

      self.get('model').updateState(this.HTTP, function () {
        self.set('statsCompleted', true);
      });

      var currentPath = '/rest/apps/' + appId + '/workflows/' + workflowId + '/current';
      var runtimePath = '/rest/apps/' + appId + '/workflows/' + workflowId + '/nextruntime';

      $.getJSON(currentPath, function (res) {
        for (var i = 0; i < self.get('elements.Actions.content').length; i++) {
          var action = self.get('elements.Actions.content')[i];
          if (res.currentStep === i) {
            action.set('isRunning', true);
            action.set('state', 'RUNNING');
          } else {
            action.set('isRunning', false);
            action.set('state', 'IDLE');
          }
          if (typeof action.getMetricsRequest === 'function') {
            action.getMetricsRequest(self.HTTP);
          }
        }
      }).fail(function() {
        for (var i = 0; i < self.get('elements.Actions.content').length; i++) {
          var action = self.get('elements.Actions.content')[i];
          action.set('isRunning', false);
          action.set('state', 'IDLE');
        }
      });

      $.getJSON(runtimePath, function (res) {
        if (!$.isEmptyObject(res)) {
          self.set('model.nextRuns', res);
        }
      });

    },

    /**
     * Lifecycle
     */
    start: function (appId, id, version, config) {

      var self = this;
      var model = this.get('model');

      model.set('currentState', 'STARTING');
      this.HTTP.post('rest', 'apps', appId, 'workflows', id, 'save', config, function (response) {

          if (response.error) {
            C.Modal.show(response.error.name, response.error.message);
          } else {
            model.set('lastStarted', new Date().getTime() / 1000);
          }

      });

    },

    /**
     * Action handlers from the View
     */
    config: function () {

      var self = this;
      var model = this.get('model');

      this.transitionToRoute('WorkflowStatus.Config');

    }

  });

  Controller.reopenClass({
    type: 'WorkflowStatus',
    kind: 'Controller'
  });

  return Controller;

});
