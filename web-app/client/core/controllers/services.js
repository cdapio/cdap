/*
 * Services Controller
 */

define([], function () {

  var ERROR_TXT = 'Instance count out of bounds.';

  var Controller = Em.Controller.extend({

    load: function () {
      var self = this;
      self.set('systemServices', []);
      self.set('userServices', []);
      self.resetServices();
      self.resetUserServices();
      this.interval = setInterval(function () {
        self.resetServices();
        self.resetUserServices();
      }, C.POLLING_INTERVAL)

    },

    config: function(appID, serviceID) {
			var self = this;
			var model = -1;
			var userServices = self.get('userServices');
			for (var i=0; i<userServices.length; i++) {
        var service = userServices[i];
        if(service.name == serviceID && service.app == appID){
          console.log(service);
          model = service;
        }
			}
      if(model == -1) {
        return;
      }

			this.transitionToRoute('Service.Config', model);
    },

    resetUserServices: function () {
      var self = this;
      var userServices = [];
      self.HTTP.rest('userServices', function(services) {
        services.map(function(service) {
          var runnablesList = [];
          service.runnables.forEach(function(runnable){
            runnablesList.push({
              "name":runnable.id,
              "requested":runnable.requested,
              "provisioned":runnable.provisioned
            });
          });
          userServices.push(C.Service.create({
            status: service.status,
            imgClass: status === 'RUNNING' ? 'complete' : 'loading',
            modelID: service.name,
            description: service.description,
            name: service.name,
            app: service.app,
            runnablesList: runnablesList,
          }));
        });
        self.set('userServices', userServices);
      });
    },

    resetServices: function () {
      var self = this;
      var systemServices = [];

      self.HTTP.rest('system/services', function (services) {
        services.map(function(service) {
          var imgSrc = service.status === 'OK' ? 'complete' : 'loading';
          var logSrc = service.status === 'OK' ? 'complete' : 'loading';
          systemServices.push(C.Service.create({
            modelId: service.name,
            description: service.description,
            id: service.name,
            name: service.name,
            description: service.description,
            status: service.status,
            min: service.min,
            max: service.max,
            isIncreaseEnabled: service.requested < service.max,
            isDecreaseEnabled: service.requested > service.min,
            logs: service.logs,
            requested: service.requested,
            provisioned: service.provisioned,
            logsStatusOk: !!(service.logs === 'OK'),
            logsStatusNotOk: !!(service.logs === 'NOTOK'),
            metricEndpoint: C.Util.getMetricEndpoint(service.name),
            metricName: C.Util.getMetricName(service.name),
            imgClass: imgSrc,
            logClass: logSrc,
          }));
        });
        self.set('systemServices', systemServices);

        // Bind all the tooltips after UI has rendered after call has returned.
        setTimeout(function () {
          $("[data-toggle='tooltip']").tooltip();
        }, 1000);
      });
    },

    start: function (appID, serviceID) {
      var self = this;
      C.Modal.show(
        "Start Service",
        "Start Service: " + appID + ":" + serviceID + "?",
        function () {
          var startURL = 'rest/apps/' + appID + '/services/' + serviceID + '/start';
          self.HTTP.post(startURL);
        }
      );
    },

    stop: function (appID, serviceID) {
      var self = this;
      C.Modal.show(
        "Stop Service",
        "Stop Service: " + appID + ":" + serviceID + "?",
        function () {
          var stopURL = 'rest/apps/' + appID + '/services/' + serviceID + '/stop';
          self.HTTP.post(stopURL);
        }
      );
    },

    getRuntimeArgs: function (appID, serviceID) {
      var payload = {};
      self.HTTP.rest('apps/' + appID + '/services/' + serviceID + '/runtimeargs');
      self.HTTP.put('rest/apps/' + appID + '/services/' + serviceID + '/runtimeargs', payload);
      return;
    },

    history: function (appID, serviceID) {
      var url = '/apps/{app-id}/services/{service-id}/history'
      self.HTTP.rest(url, callBackFunction);
    },

    liveinfo: function (appID, serviceID) {
      var url = '/apps/{app-id}/services/{service-id}/live-info'
      self.HTTP.rest(url, callBackFunction);
    },

    userService_increaseInstance: function (appID, serviceID, runnableID, instanceCount) {
      var self = this;
      console.log('appID: ' + appID);
      console.log('serviceID: ' + serviceID);
      console.log('runnableID: ' + runnableID);
      console.log('instanceCount: ' + instanceCount);
      C.Modal.show(
        "Increase instances",
        "Increase instances for " + appID + ":" + serviceID + ":" + runnableID + "?",
        function () {

          var payload = {data: {instances: ++instanceCount}};
          var sMap = self.servicesMap;
          var service = sMap[[appID,serviceID]];
//          if (instanceCount > service.max || instanceCount < service.min) {
//            C.Util.showWarning(ERROR_TXT);
//            return;
//          }
          self.userService_executeInstanceCall(appID, serviceID, runnableID, payload);
        });
    },
    userService_decreaseInstance: function (appID, serviceID, runnableID, instanceCount) {
      var self = this;
      console.log('appID: ' + appID);
      console.log('serviceID: ' + serviceID);
      console.log('runnableID: ' + runnableID);
      console.log('instanceCount: ' + instanceCount);
      C.Modal.show(
        "Increase instances",
        "Increase instances for " + appID + ":" + serviceID + ":" + runnableID + "?",
        function () {

          var payload = {data: {instances: --instanceCount}};
          var sMap = self.servicesMap;
          var service = sMap[[appID,serviceID]];
          console.log(instanceCount);
          if (instanceCount <= 0) {
            C.Util.showWarning(ERROR_TXT);
            return;
          }
          self.userService_executeInstanceCall(appID, serviceID, runnableID, payload);
        });
    },
    userService_executeInstanceCall: function(appID, serviceID, runnableID, payload) {
      var self = this;
      var url = 'rest/apps/' + appID + '/services/' + serviceID + '/runnables/' + runnableID + '/instances';
      this.HTTP.put(url, payload,
        function(resp, status) {
        if (status === 'error') {
          C.Util.showWarning(resp);
        } else {
          self.resetServices();
        }
      });
    },

    increaseInstance: function (serviceName, instanceCount) {
      var self = this;
      C.Modal.show(
        "Increase instances",
        "Increase instances for " + serviceName + "?",
        function () {

          var payload = {data: {instances: --instanceCount}};
          var services = self.get('systemServices');
          for (var i = 0; i < services.length; i++) {
            var service = services[i];
            if (service.name === serviceName) {
              if (instanceCount > service.max || instanceCount < service.min) {
                C.Util.showWarning(ERROR_TXT);
                return;
              }
            }
          }
          self.executeInstanceCall(serviceName, payload);
        });
    },

    decreaseInstance: function (serviceName, instanceCount) {
      var self = this;
      C.Modal.show(
        "Decrease instances",
        "Decrease instances for " + serviceName + "?",
        function () {

          var payload = {data: {instances: --instanceCount}};
          var services = self.get('systemServices');
          for (var i = 0; i < services.length; i++) {
            var service = services[i];
            if (service.name === serviceName) {
              if (instanceCount > service.max || instanceCount < service.min) {
                C.Util.showWarning(ERROR_TXT);
                return;
              }
            }
          }
          self.executeInstanceCall(serviceName, payload);
        });  
    },

    executeInstanceCall: function(serviceName, payload) {
      var self = this;
      this.HTTP.put('rest/system/services/' + serviceName + '/instances', payload,
        function(resp, status) {
        if (status === 'error') {
          C.Util.showWarning(resp);
        } else {
          self.resetServices();
        }
      });
    },

    unload: function () {
      clearInterval(this.interval);
    }

  });

  Controller.reopenClass({
    type: 'Services',
    kind: 'Controller'
  });

  return Controller;

});
