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
        model.actions[i].completionPercentage = 50;
        model.actions[i].id = model.actions[i].name.replace(' ', '');
        this.get('elements.Actions.content').push(Em.Object.create(model.actions[i]));      
      }

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
        self.connectEntities();
      }, C.EMBEDDABLE_DELAY);

    },

    unload: function () {

      clearInterval(this.interval);

    },

    connectEntities: function() {
      var actions = this.get('elements.Actions.content').map(function (item) {
<<<<<<< HEAD
        return item.id || item.get('id');
=======
        return item.name || item.get('name');
>>>>>>> feature/workflow
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

      this.HTTP.rest('apps', appId, 'workflows', workflowId, 'status', function (response) {
        if (!jQuery.isEmptyObject(response)) {
          self.set('model.currentState', response.status);
        }
        self.set('statsCompleted', true);
        var path = '/rest/apps/' + appId + '/workflows/' + workflowId + '/current';

        jQuery.getJSON(path, function (res) {
          for (var i = 0; i < self.get('elements.Actions.content').length; i++) {
            var action = self.get('elements.Actions.content')[i];
            if (res.currentStep === i) {
              action.set('isRunning', true); 
              action.set('state', 'RUNNING'); 
            } else {
              action.set('isRunning', false);
              action.set('state', 'IDLE'); 
            }
          }
        }).fail(function() {
          for (var i = 0; i < self.get('elements.Actions.content').length; i++) {
            var action = self.get('elements.Actions.content')[i];
            action.set('isRunning', false);
            action.set('state', 'IDLE'); 
          }
        });

      });

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
