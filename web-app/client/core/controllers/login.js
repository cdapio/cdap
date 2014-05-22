/*
 * Login Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    load: function () {
      this.set('warning', '')
    },

    isValid: function () {
      if (!this.get('username') || !this.get('password')) {
        this.set('warning', 'You must specify username and password.')
        return false;
      }
      return true;
    },

    unload: function () {
      //pass
    }

  });

  Controller.reopenClass({
    type: 'Login',
    kind: 'Controller'
  });

  return Controller;

});