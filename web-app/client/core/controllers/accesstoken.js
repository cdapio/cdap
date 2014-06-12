/*
 * Login Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    load: function () {
      this.set('warning', '');
      this.set('username', '');
    },

    isValid: function () {
      if (!this.get('username') || !this.get('password')) {
        this.set('warning', 'You must specify username and password.');
        return false;
      }
      return true;
    },

    submit: function() {
      var self = this;
      console.log('here');
      this.HTTP.post('accesstoken', {}, function(responseData, status) {
        self.set('warning', responseData);
      });
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