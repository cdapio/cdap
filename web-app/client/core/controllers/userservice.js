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

    },


    runnableIncreaseInstance: function (service, runnableID, instanceCount) {
      this.runnableVerifyInstanceBounds(service, runnableID, ++instanceCount, "Increase");
    },
    runnableDecreaseInstance: function (service, runnableID, instanceCount) {
      this.runnableVerifyInstanceBounds(service, runnableID, --instanceCount, "Decrease");
    },

    runnableVerifyInstanceBounds: function (service, runnableID, numRequested, direction) {
      var self = this;
      if (numRequested <= 0) {
        C.Modal.show("Instances Error", ERROR_TXT);
        return;
      }
      C.Modal.show(
        direction + " instances",
        direction + " instances for runnable: " + runnableID + "?",
        function () {
          var url = 'rest/apps/' + service.app + '/services/' + service.name + '/runnables/' + runnableID + '/instances';
          self.executeInstanceCall(url, numRequested);
        }
      );
    }

  });

  Controller.reopenClass({
    type: 'Userservice',
    kind: 'Controller'
  });

  return Controller;

});
