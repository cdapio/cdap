/*
 * Login Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    load: function () {
      //pass
    },

    unload: function () {
      //pass
    }

  });

  Controller.reopenClass({
    type: 'AccessToken',
    kind: 'Controller'
  });

  return Controller;

});