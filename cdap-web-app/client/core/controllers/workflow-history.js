/*
 * Workflow History Controller
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
        model.actions[i].running = false;
        model.actions[i].appId = self.get('model').app;
        model.actions[i].divId = model.actions[i].programName.replace(' ', '');

        if (model.actions[i].properties && 'mapReduceName' in model.actions[i].properties) {
          var transformedModel = C.Mapreduce.transformModel(model.actions[i]);

          this.get('elements.Actions.content').push(C.Mapreduce.create(transformedModel));
        } else {
          this.get('elements.Actions.content').push(Em.Object.create(model.actions[i]));
        }

      }

      this.HTTP.rest('apps', model.app, 'workflows', model.name, 'runs', function (response) {
          if (response) {
            var history = response;

            for (var i = 0; i < history.length; i ++) {
              if (history[i]["status"] != "RUNNING") {
                self.runs.pushObject(C.Run.create(history[i]));
              }
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