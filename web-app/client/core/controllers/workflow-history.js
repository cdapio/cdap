/*
 * Flow History Controller
 */

define([], function () {

  var Controller = Ember.Controller.extend({

    runs: Ember.ArrayProxy.create({
      content: []
    }),

    elements: Em.Object.create(),

    load: function () {
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

      this.HTTP.rest('apps', model.app, 'workflows', model.name, 'history', function (response) {
          if (response) {
            var history = response;

            for (var i = 0; i < history.length; i ++) {

              self.runs.pushObject(C.Run.create(history[i]));

            }
          }

      });

    },

    unload: function () {

      this.set('elements.Actions.content', []);

      this.get('runs').set('content', []);

    }
  });

  Controller.reopenClass({
    type: 'WorkflowHistory',
    kind: 'Controller'
  });

  return Controller;

});