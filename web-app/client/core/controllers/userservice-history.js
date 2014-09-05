/*
 * Userservice History Controller
 */

define([], function () {

  var Controller = Ember.Controller.extend({

    needs: ['Userservice'],
    runs: Ember.ArrayProxy.create({
      content: []
    }),

    elements: Em.Object.create(),

    load: function () {
      var model = this.get('model');
      var self = this;

      if(!model) {
        return;
      }

      this.HTTP.rest('apps', model.app, 'services', model.name, 'history', function (response) {

          if (response) {
            var history = response;

            for (var i = 0; i < history.length; i ++) {

              self.runs.pushObject(C.Run.create(history[i]));

            }
          }

      });

    },

    unload: function () {

      this.get('runs').set('content', []);

    }
  });

  Controller.reopenClass({
    type: 'UserserviceHistory',
    kind: 'Controller'
  });

  return Controller;

});