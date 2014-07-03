/*
 * Workflow Status Controller
 */

define(['helpers/plumber'], function (Plumber) {

  var QUARTZ_USER = 'DEFAULT';

  var Controller = Ember.Controller.extend({

    elements: Em.Object.create(),
    suspended: false,

    load: function () {

      this.clearTriggers(true);
      var model = this.get('model');
      var self = this;
      this.set('elements.Action', Em.ArrayProxy.create({content: []}));
      this.set('elements.Schedule', Em.ArrayProxy.create({content: []}));

      for (var i = 0; i < model.actions.length; i++) {
        model.actions[i].state = 'IDLE';
        model.actions[i].running = false;

        model.actions[i].appId = self.get('model').app;
        model.actions[i].divId = model.actions[i].name.replace(' ', '');

        if (model.actions[i].properties && 'mapReduceName' in model.actions[i].properties) {
          var transformedModel = C.Mapreduce.transformModel(model.actions[i]);
          var mrModel = C.Mapreduce.create(transformedModel);
          this.get('elements.Action.content').push(mrModel);
        } else {
          this.get('elements.Action.content').push(Em.Object.create(model.actions[i]));
        }

      }

      this.HTTP.rest(model.get('context') + '/schedules', function (all) {

        self.get('elements.Schedule').clear();

        var i = all.length, schedules = [];
        while (i--) {
          schedules.unshift(Em.Object.create({ id: all[i] }));
        }

        self.get('elements.Schedule').pushObjects(schedules);

        self.updateNextRunTime();

      });

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

    updateNextRunTime: function () {

      var model = this.get('model');
      var self = this;

      self.HTTP.rest(model.get('context') + '/nextruntime', function (all) {

        var next = Infinity;
        var i = all.length, schedules = [];
        while (i--) {

          all[i].id = all[i].id.replace(QUARTZ_USER + '.', '');

          if (all[i].time < next) {
            next = all[i].time;
          }

          self.get('elements.Schedule').forEach(function (item, index) {
            if (item.id === all[i].id) {
              item.set('time', new Date(next).toLocaleString());
            }
          });

        }

        if (next !== Infinity) {
          self.set('nextRun', +next);
          self.set('nextRunLabel', new Date(next).toLocaleString());
        } else {
          self.set('nextRun', -1);
          self.set('nextRunLabel', 'None');
        }

        var timeout = +next - new Date().getTime();
        if (timeout < 1000) {
          timeout = 1000;
        }

        setTimeout(function () {
          self.updateNextRunTime();
        }, timeout);

      });

    },

    unload: function () {

      clearInterval(this.interval);
      this.set('elements.Action.content', []);

    },

    connectEntities: function() {
      var actions = this.get('elements.Action.content').map(function (item) {
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

    __previousState: null,

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

        // Hax for not showing warning message when call fails.
        $.get(model.get('context') + '/current').done(function (run) {
          var activeAction = run.currentStep;
          self.get('elements.Action').forEach(function (action, index) {
            if (index === activeAction) {
              action.set('currentState', 'RUNNING');
            } else {
              action.set('currentState', 'STOPPED');
            }
          });
        }).fail(function () {
          self.get('elements.Action').forEach(function (action, index) {
            action.set('currentState', 'STOPPED');
          });
        });

        this.set('__previousState', 'RUNNING');

      } else {

        if (this.get('__previousState') === 'RUNNING') {

          self.get('elements.Action').forEach(function (action, index) {
            action.set('currentState', 'STOPPED');
          });

        }

        this.set('__previousState', 'STOPPED');

      }

      var self = this;
      var context = this.get('model.context');

      this.get('elements.Schedule').forEach(function (schedule, index) {
        self.HTTP.rest(context, 'schedules', schedule.id, 'status', function (status) {

          if (status.status === 'SUSPENDED') {
            self.set('suspended', true);
          } else {
            self.set('suspended', false);
          }

        });
      });

      var next = this.get('nextRun');

      if (next !== -1) {

        var days, hours, minutes, seconds, remaining = (next - new Date().getTime()) / 1000;

        if (remaining <= 0 || isNaN(remaining)) {

          self.set('timeToNextRun', '00:00:00');

        } else {

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

      }

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

    resume: function () {

      var self = this;
      var context = this.get('model.context');
      var total = this.get('schedules.length');

      this.get('elements.Schedule').forEach(function (schedule, index) {
        self.HTTP.rpc(context, 'schedules', schedule.id, 'resume', function () {
          if (!--total) {
            self.set('suspended', false);
          }
        });
      });

    },

    suspend: function () {

      var self = this;
      var context = this.get('model.context');
      var total = this.get('schedules.length');

      this.get('elements.Schedule').forEach(function (schedule, index) {
        self.HTTP.rpc(context, 'schedules', schedule.id, 'suspend', function () {
          if (!--total) {
            self.set('suspended', true);
          }
        });
      });

    },

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
