/*
 * Services Controller
 */

define([], function () {

  var ERROR_TXT = 'Instance count out of bounds.';

  var Controller = Em.Controller.extend({

    load: function () {
      var self = this;
      self.set('services', []);
      self.resetServices();
    },

    resetServices: function () {
      var self = this;
      self.HTTP.rest('system/services', function (services) {
        var servicesArr = [];
        services.map(function(service) {
          servicesArr.push(C.Service.create({
            modelId: service.name,
            id: service.name,
            name: service.name,
            status: service.status,
            min: service.min,
            max: service.max,
            cur: service.cur,
            logs: service.logs,
            statusOk: !!(service.status === 'OK'),
            statusNotOk: !!(service.status === 'NOT OK'),
            logsStatusOk: !!(service.logs === 'OK'),
            logsStatusNotOk: !!(service.logs === 'NOT OK'),
            metricEndpoint: C.Util.getMetricEndpoint(service.name),
            metricName: C.Util.getMetricName(service.name)
          }));
        });
        self.set('services', servicesArr);
      });
    },

    increaseInstance: function (serviceName, instanceCount) {
      if (confirm("Increase instances for " + serviceName + "?")) {
        var self = this;
        var payload = {data: {instances: ++instanceCount}};
        var services = self.get('services');
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
      }      
    },

    decreaseInstance: function (serviceName, instanceCount) {
      if (confirm("Decrease instances for " + serviceName + "?")) {
        var self = this;
        var payload = {data: {instances: --instanceCount}};
        var services = self.get('services');
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
      }      
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
