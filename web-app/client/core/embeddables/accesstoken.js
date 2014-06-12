/*
 * Login view.
 */

define([], function () {

  var Embeddable = Em.View.extend({

    templateName: 'access-token-view',

    didInsertElement: function() {
      this._super();
      var self = this;

      $("#access-token-submit-btn").click(function (e) {
        if (self.get('controller').isValid()) {
          var username = self.get('controller.username');
          var password = self.get('controller.password');

          // Precheck to see if the username and password are valid since we have no other way of
          // displaying the error.
          $.post('/validatelogin', {
            username: username,
            password: password
          }).done(function (response) {

            $("#access-token-form").submit();

          }).fail(function (response) {

            self.set('controller.warning', 'Invalid credentials.')
          });
        }
      });

      // Workaround to make enter to click.
      $('#access-token-form').each(function() {
        $(this).find('input').keypress(function(e) {
            // Enter pressed?
            if(e.which == 10 || e.which == 13) {
              $("#access-token-submit-btn").click();
            }
        });
      });
    }
  });

  Embeddable.reopenClass({

    type: 'AccessToken',
    kind: 'Embeddable'

  });

  return Embeddable;

  });