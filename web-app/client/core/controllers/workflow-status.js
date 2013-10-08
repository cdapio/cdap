/*
 * Workflow Status Controller
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
          var transformedModel = C.Mapreduce.transformModel(model.actions[i]);
          var mrModel = C.Mapreduce.create(transformedModel);
          this.get('elements.Actions.content').push(mrModel);
        } else {
          this.get('elements.Actions.content').push(Em.Object.create(model.actions[i]));
        }

      }

      this.updateNextRunTime();

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

    schedule: Em.ArrayProxy.create({ content: [] }),

    updateNextRunTime: function () {

      var model = this.get('model');
      var self = this;

      self.HTTP.rest(model.get('context') + '/nextruntime', function (all) {

        var next = Infinity;
        var i = all.length, schedule = [];
        while (i--) {
          if (all[i].time < next) {
            next = all[i].time;
          }
          schedule.unshift(new Date(next).toLocaleString());
        }

        self.set('schedule.content', schedule);

        if (next !== Infinity) {
          self.set('nextRun', +next);
          self.set('nextRunLabel', new Date(next).toLocaleString());
        } else {
          self.set('nextRun', -1);
          self.set('nextRunLabel', 'None');
        }

        setTimeout(function () {
          self.updateNextRunTime();
        }, +next - new Date().getTime());

      });

    },

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

      self.get('model').updateState(this.HTTP, function () {
        self.set('statsCompleted', true);
      });

      var model = this.get('model');
      if (model.get('currentState') === 'RUNNING') {

        this.HTTP.rest(model.get('context') + '/current', function (run) {

          var activeAction = run.currentStep;
          self.get('elements.Actions').forEach(function (action, index) {
            if (index === activeAction) {
              action.set('currentState', 'RUNNING');
            } else {
              action.set('currentState', 'STOPPED');
            }
          });

        });

      }

      var next = this.get('nextRun');

      if (next !== -1) {

        var days, hours, minutes, seconds, remaining = (next - new Date().getTime()) / 1000;

        days = Math.floor(remaining / 86400);
        remaining = remaining % 86400;

        hours = Math.floor(remaining / 3600);
        remaining = remaining % 3600;

        minutes = Math.floor(remaining / 60);
        seconds = Math.floor(remaining % 60);

        if (days > 0) {
          self.set('timeToNextRun', days + ' Days');
        } else {
          self.set('timeToNextRun', (hours < 10 ? '0' : '') + hours + ':' +
            (minutes < 10 ? '0' : '') + minutes + ':' +
            (seconds < 10 ? '0' : '') + seconds);
        }

      }

    },

    /**
     * Lifecycle
     */
    start: function (appId, id, config) {

      var self = this;
      var model = this.get('model');

      model.set('currentState', 'STARTING');

      this.HTTP.post('rest', 'apps', appId, 'workflows', id, 'start', {
        data: config
      }, function (response) {

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

    },

    loadAction: function (action) {

      if (action.get('type') === 'Mapreduce') {
        this.transitionToRoute('MapreduceStatus', action);
      }

    }

  });

  Controller.reopenClass({
    type: 'WorkflowStatus',
    kind: 'Controller'
  });

  return Controller;

});
