/*
 * Services Controller
 */

define([], function () {

  var ERROR_TXT = 'Instance count out of bounds.';

  var Controller = Em.Controller.extend({

    load: function () {
      var self = this;
      self.set('systemServices', []);
      self.set('userServices2', []);

      self.resetServices();
      this.interval = setInterval(function () {
        self.resetServices();
      }, C.POLLING_INTERVAL)

    },

    resetServices: function () {
      var self = this;
      var systemServices = [];
      var userServices = [];
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
            statusOk: !!(service.status === 'OK'),
            statusNotOk: !!(service.status === 'NOTOK'),
            logsStatusOk: !!(service.logs === 'OK'),
            logsStatusNotOk: !!(service.logs === 'NOTOK'),
            metricEndpoint: C.Util.getMetricEndpoint(service.name),
            metricName: C.Util.getMetricName(service.name),
            imgClass: imgSrc,
            logClass: logSrc,
            //appID: service.app, //system services have no associated app.
          }));
        });
        self.set('systemServices', systemServices);

        // Bind all the tooltips after UI has rendered after call has returned.
        setTimeout(function () {
          $("[data-toggle='tooltip']").tooltip();
        }, 1000);
      });

      self.HTTP.rest('apps', function (apps) {
        apps.forEach(function(app) {
          var appUrl = 'apps/' + app.name + '/services';
          self.HTTP.rest(appUrl, function (services) {
            services.map(function(service) {
              //var imgSrc = service.status === 'OK' ? 'complete' : 'loading';
              //var logSrc = service.status === 'OK' ? 'complete' : 'loading';
///*

              var statusCheckURL = appUrl + '/' + service.name + '/status';
              self.HTTP.rest(statusCheckURL, function(f) {
                var status = f.status;
                for (var i=0; i < userServices.length; i++) {
                  if  (userServices[i].get('name') != service.name) {
                    continue;
                  }
                  userServices[i].set('status', status);
                  userServices[i].set('statusOk', !!(status === 'RUNNING'));
                  userServices[i].set('statusNotOk', !!(status === 'STOPPED'));
                  userServices[i].set('imgClass', status === 'RUNNING' ? 'complete' : 'loading');
                }
                self.set('userServices', userServices);
              });

              var instancesCheckUrl = appUrl + '/' + service.name + '/runnables' + '/CatalogService' + '/instances';
              self.HTTP.rest(instancesCheckUrl, function(f) {
                for (var i=0; i < userServices.length; i++) {
                  if  (userServices[i].get('name') != service.name) {
                    continue;
                  }
                  userServices[i].set('requested', f.requested);
                  userServices[i].set('provisioned', f.provisioned);
                }
                self.set('userServices', userServices);
              });
              //*/
              console.log('pushed');
              userServices.push(C.Service.create({
                modelId: service.name,
                description: service.description,
                id: service.name,
                name: service.name,
                description: service.description,
                //status: service.status,
                //min: service.min,
                //max: service.max,
                //isIncreaseEnabled: service.requested < service.max,
                //isDecreaseEnabled: service.requested > service.min,
                //logs: service.logs,
                //requested: service.requested,
                //provisioned: service.provisioned,
                //statusOk: !!(service.status === 'OK'),
                //statusNotOk: !!(service.status === 'NOTOK'),
                //logsStatusOk: !!(service.logs === 'OK'),
                //logsStatusNotOk: !!(service.logs === 'NOTOK'),
                metricEndpoint: C.Util.getMetricEndpoint(service.name),
                metricName: C.Util.getMetricName(service.name),
                //imgClass: imgSrc,
                //logClass: logSrc,
                appID: service.app,
              }));
            });


          });
        });

        // Bind all the tooltips after UI has rendered after call has returned.
        setTimeout(function () {
          $("[data-toggle='tooltip']").off()
          $("[data-toggle='tooltip']").tooltip();
        }, 1000);

      });

    },

    start: function (appID, serviceID) {
      var self = this;
      C.Modal.show(
        "Start Service",
        "Start Service: " + appID + "?",
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
        "Stop Service: " + appID + "?",
        function () {
          var stopURL = 'rest/apps/' + appID + '/services/' + serviceID + '/stop';
          self.HTTP.post(stopURL);
        }
      );
    },

    setRuntimeArgs: function () {

    },

    increaseInstance: function (serviceName, instanceCount) {
      var self = this;
      C.Modal.show(
        "Increase instances",
        "Increase instances for " + serviceName + "?",
        function () {

          var payload = {data: {instances: ++instanceCount}};
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
