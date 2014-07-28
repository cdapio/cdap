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
          var url = 'rest/apps/' + service.app + '/services/' + service.name 
              + '/runnables/' + runnableID + '/instances';
          self.executeInstanceCall(url, numRequested);
        }
      );
    },

    start: function (service) {
      var self = this;
      if (service.status == "RUNNING") {
        C.Util.showWarning("Program is already running.");
        return;
      }
      C.Modal.show(
        "Start Service",
        "Start Service: " + service.app + ":" + service.name + "?",
        function () {
          var startURL = 'rest/apps/' + service.app + '/services/' + service.name + '/start';
          self.HTTP.post(startURL, function() {
            service.update(self.HTTP);
          });
        }
      );
    },

    stop: function (service) {
      var self = this;
      if (service.status == "STOPPED") {
        C.Util.showWarning("Program is already stopped.");
        return;
      }
      C.Modal.show(
        "Stop Service",
        "Stop Service: " + service.app + ":" + service.name + "?",
        function () {
          var stopURL = 'rest/apps/' + service.app + '/services/' + service.name + '/stop';
          self.HTTP.post(stopURL, function() {
            service.update(self.HTTP);
          });
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
