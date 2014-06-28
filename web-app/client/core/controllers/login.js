/*
 * Login Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    load: function () {
      // Hide everything for this route.
      Em.run.next(function() {
        $("#global").hide();
        $("#nav").hide();
        $("#content-body").css({"padding-left": 0});
      });



      this.set('warning', '');
    },

    isValid: function () {
      if (!this.get('username') || !this.get('password')) {
        this.set('warning', 'You must specify username and password.');
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