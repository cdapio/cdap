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

      var flowlets = model.flowlets;
      var objects = [];
      for (var i = 0; i < flowlets.length; i ++) {
        objects.push(C.Flowlet.create(flowlets[i]));
      }
      this.set('elements.Flowlet', Em.ArrayProxy.create({content: objects}));

      var streams = model.flowStreams;
      objects = [];
      for (var i = 0; i < streams.length; i ++) {
        objects.push(C.Stream.create(streams[i]));
      }
      this.set('elements.Stream', Em.ArrayProxy.create({content: objects}));
      this.set('elements.Batch', Em.ArrayProxy.create({content: objects}));

      this.HTTP.rest('apps', model.app, 'flows', model.name, 'history',
          function (response) {

            if (response) {
              var history = response;

              for (var i = 0; i < history.length; i ++) {

                self.runs.pushObject(C.Run.create(history[i]));

              }
            }

      });

    },

    unload: function () {

      this.set('elements.Flowlet', Em.Object.create());
      this.set('elements.Stream', Em.Object.create());
      this.set('elements.Batch', Em.Object.create());

      this.get('runs').set('content', []);

    }
  });

  Controller.reopenClass({
    type: 'WorkflowHistory',
    kind: 'Controller'
  });

  return Controller;

});