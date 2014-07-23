/*
 * User service controller.
 */

define([], function () {

  var Controller = Ember.Controller.extend({

    load: function () {

      //pass

    },

    unload: function () {

      clearInterval(this.interval);

    }

  });

  Controller.reopenClass({
    type: 'Userservice',
    kind: 'Controller'
  });

  return Controller;

});
