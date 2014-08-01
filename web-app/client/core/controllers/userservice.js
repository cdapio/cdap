/*
 * User service controller.
 */

define(['core/controllers/services'], function (servicesController) {

  var ERROR_TXT = 'Requested Instance count out of bounds.';
  
  var Controller = servicesController.extend({
    needs: ['Userservice'],

    config: function () {
      var model = this.get('model');
      this.transitionToRoute('UserserviceStatus.Config');
    },

    load: function () {
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
          var url = 'rest/apps/' + service.app + '/services/' + service.name 
              + '/runnables/' + runnableID + '/instances';
          var callback =  function(){service.update(self.HTTP)};
          self.executeInstanceCall(url, numRequested, callback);
        }
      );
    },


    exec: function () {

      var model = this.get('model');
      var action = model.get('defaultAction');
      if (action && action.toLowerCase() in model) {
        model[action.toLowerCase()](this.HTTP);
      }

    },

  });

  Controller.reopenClass({
    type: 'Userservice',
    kind: 'Controller'
  });

  return Controller;

});
