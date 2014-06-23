/*
 * Access Token Controller used to give users long-lasting tokens. 
 */

define([], function () {

  var LOGIN_WARNING = 'Could not authenticate credentials.';

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
        if (status === 200) {
          self.set('warning', '');
          self.set('token', responseData.access_token);
          self.set('token_expires', responseData.expires_in);  
        } else {
          self.set('warning', LOGIN_WARNING);
        }        
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