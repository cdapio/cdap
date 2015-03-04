/*
 * Services Controller
 */

define([], function () {

  var Controller = Em.Controller.extend({

    load: function () {
      var self = this;
      self.set('systemServices', []);

      self.fetchServices();

      this.interval = setInterval(function () {
        self.updateServices();
      }, C.POLLING_INTERVAL)
    },

    fetchServices: function () {
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
            logClass: logSrc
          }));
        });
        self.set('systemServices', systemServices);

        // Bind all the tooltips after UI has rendered after call has returned.
        setTimeout(function () {
          $("[data-toggle='tooltip']").tooltip();
          $('input,textarea').focus(function(){
             $(this).data('placeholder',$(this).attr('placeholder'))
             $(this).attr('placeholder','');
          });
          $('input,textarea').blur(function(){
             $(this).attr('placeholder',$(this).data('placeholder'));
          });
        }, 1000);
      });
    },

    updateServices: function () {
      var self = this;
      self.HTTP.rest('system/services', function (services) {
        services.map(function(service) {
          var existingService = self.find(service.name);

          existingService.set('modelId', service.name);
          existingService.set('description', service.description);
          existingService.set('id', service.name);
          existingService.set('name', service.name);
          existingService.set('description', service.description);
          existingService.set('status', service.status);
          existingService.set('min', service.min);
          existingService.set('max', service.max);
          existingService.set('isIncreaseEnabled', service.requested < service.max);
          existingService.set('isDecreaseEnabled', service.requested > service.min);
          existingService.set('logs', service.logs);
          existingService.set('requested', service.requested);
          existingService.set('provisioned', service.provisioned);
          existingService.set('logsStatusOk', !!(service.logs === 'OK'));
          existingService.set('logsStatusNotOk', !!(service.logs === 'NOTOK'));
          existingService.set('metricEndpoint', C.Util.getMetricEndpoint(service.name));
          existingService.set('metricName', C.Util.getMetricName(service.name));
          existingService.set('imgClass', service.status === 'OK' ? 'complete' : 'loading');
          existingService.set('logClass', service.status === 'OK' ? 'complete' : 'loading');
        });

        // Bind all the tooltips after UI has rendered after call has returned.
        setTimeout(function () {
          $("[data-toggle='tooltip']").tooltip();
        }, 1000);
      });
    },

    find: function (serviceName) {
      var systemServices = this.get('systemServices');
      for (var i=0; i<systemServices.length; i++) {
        var service = systemServices[i];
        if(service.name === serviceName){
          return service;
        }
      }
    },

    keyPressed: function (evt) {
      var btn = this.$().parent().parent().next().children();
      var inp = this.value;
      return C.Util.handleInstancesKeyPress(btn, inp, this.placeholder);
    },

    changeInstances: function (service) {
      var inputStr = service.get('instancesInput');
      var input = parseInt(inputStr);

      service.set('instancesInput', '');
      setTimeout(function () {
        $('.services-instances-input').keyup();
      },500);

      if (service && service.requested === input) {
        return; //no-op
      }

      this.setInstances(service, input);
    },

    setInstances: function(service, numRequested) {
      var self = this;
      C.Modal.show(
        "Request " + numRequested + " instances",
        "Request " + numRequested + " instances for " + service.name + "?",
        function () {
          var callback = function(){ self.updateServices() };
          self.executeInstanceCall('rest/system/services/' + service.name + '/instances', numRequested, callback);
        }
      );
    },

    executeInstanceCall: function (url, numRequested, callback) {
      var self = this;
      var payload = {data: {instances: numRequested}};
      this.HTTP.put(url, payload,
        function(resp, status) {
        if (status === 'error') {
          C.Util.showWarning(resp);
        } else {
          if (typeof(callback) == "function") {
            callback();
          }
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
