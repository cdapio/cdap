/*
 * User service controller.
 */

define(['core/controllers/services'], function (servicesController) {
  var Controller = servicesController.extend({
    needs: ['Userservice'],

    config: function () {
      var model = this.get('model');
      console.log('transitioning');
      this.transitionToRoute('UserserviceStatus.Config');
    },

    load: function () {
      //pass
      var self = this;
      var parent = this.get('needs')[0];
      var model = this.get('controllers').get(parent).get('model');

      this.interval = setInterval(function () {
        if (model) {
          model.update(self.HTTP);
        }
      }, C.POLLING_INTERVAL)
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
