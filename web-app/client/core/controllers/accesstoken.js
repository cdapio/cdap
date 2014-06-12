/*
 * Login Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    load: function () {
      this.set('warning', '');
      this.set('username', '');
      this.set('password', '');
      this.set('token', '');
      this.set('token_expires', '');
    },

    submit: function() {
      var self = this;
      data = { 'username': self.get('username'), 'password': self.get('password') };
      this.HTTP.post('accesstoken', data, function(responseData, status) {
        self.set('token', responseData.access_token);
        self.set('token_expires', responseData.expires_in);
      });
    },

    unload: function () {
      this.set('token', '');
      this.set('token_expires', '');
    }

  });

  Controller.reopenClass({
    type: 'AccessToken',
    kind: 'Controller'
  });

  return Controller;

});