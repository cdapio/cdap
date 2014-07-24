/*
 * User service controller.
 */

define([], function () {

  var Controller = Ember.Controller.extend({
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

    userService_increaseInstance: function (service, runnableID, instanceCount) {
      var self = this;
      var appID = service.app;
      var serviceID = service.name;
      C.Modal.show(
        "Increase instances",
        "Increase instances for " + appID + ":" + serviceID + ":" + runnableID + "?",
        function () {
          var payload = {data: {instances: ++instanceCount}};
          self.userService_executeInstanceCall(service, runnableID, payload);
        });
    },
    userService_decreaseInstance: function (service, runnableID, instanceCount) {
      var self = this;
      var appID = service.app;
      var serviceID = service.name;
      C.Modal.show(
        "Increase instances",
        "Increase instances for " + appID + ":" + serviceID + ":" + runnableID + "?",
        function () {
          var payload = {data: {instances: --instanceCount}};
          if (instanceCount <= 0) {
            C.Util.showWarning(ERROR_TXT);
            return;
          }
          self.userService_executeInstanceCall(service, runnableID, payload);
        });
    },
    userService_executeInstanceCall: function(service, runnableID, payload) {
      var self = this;
      var appID = service.app;
      var serviceID = service.name;
      var url = 'rest/apps/' + appID + '/services/' + serviceID + '/runnables/' + runnableID + '/instances';
      this.HTTP.put(url, payload,
        function(resp, status) {
        if (status === 'error') {
          C.Util.showWarning(resp);
        } else {
          service.update(self.HTTP);
        }
      });
    },
    start: function (service) {
      var self = this;
      var appID = service.app;
      var serviceID = service.name;
      C.Modal.show(
        "Start Service",
        "Start Service: " + appID + ":" + serviceID + "?",
        function () {
          if (service.status == "RUNNING") {
            C.Util.showWarning("Program is already running.");
            return;
          }
          var startURL = 'rest/apps/' + appID + '/services/' + serviceID + '/start';
          self.HTTP.post(startURL, function() {
            service.update(self.HTTP);
          });
        }
      );
    },

    stop: function (service) {
      var self = this;
      var appID = service.app;
      var serviceID = service.name;
      C.Modal.show(
        "Stop Service",
        "Stop Service: " + appID + ":" + serviceID + "?",
        function () {
          if (service.status == "STOPPED") {
            C.Util.showWarning("Program is already stopped.");
            return;
          }
          var stopURL = 'rest/apps/' + appID + '/services/' + serviceID + '/stop';
          self.HTTP.post(stopURL, function() {
            service.update(self.HTTP);
          });
        }
      );
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
