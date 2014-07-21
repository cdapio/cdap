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
      self.servicesMap = {};
      self.resetServices();
      this.interval = setInterval(function () {
        self.resetServices();
      }, C.POLLING_INTERVAL)

    },

    servicesExist: function () {
      return true;
    }.property('userServices'),

    servicesArray:  function () {
      var self = this;
      var returnArray = [];
      var input = self.servicesMap;

      var keys = Object.keys(input);
      for (var i=0; i < keys.length; i++) {
        var key = keys[i];
        var obj = input[key];
        returnArray.pushObject(obj);
      }
      return returnArray;
    }.property('userServices'),

    updateRunnable: function (app, service, runnable) {
      var self = this;
      var sMap = self.servicesMap;

      var runnableStatusURL = 'apps/' + app.name + '/services' + '/' + service.name + '/runnables' + '/' + runnable + '/instances';
      self.HTTP.rest(runnableStatusURL, function(f) {

        var list = sMap[[app.name,service.name]].runnablesList;
        var map2 = sMap[[app.name,service.name]].runnablesMap;
        if(map2[runnable] == undefined) {
          map2[runnable] = {};
        }

        map2[runnable].name = runnable;
        map2[runnable].requested = f.requested;
        map2[runnable].provisioned = f.provisioned;

        list.clear();
        var keys = Object.keys(map2);
        for (var m=0; m < keys.length; m++) {
          var val = map2[keys[m]];
          list.pushObject(val);
        }
      });
    },

    updateService: function (app, service) {
      var self = this;
      var userServices = self.get('userServices');
      var sMap = self.servicesMap;

      var runnableNameURL = 'apps/' + app.name + '/services' + '/' + service.name;
      self.HTTP.rest(runnableNameURL, function(f) {
        f.runnables.forEach( function(runnable) {
          self.updateRunnable(app, service, runnable);
        });
      });

      var statusCheckURL = 'apps/' + app.name + '/services' + '/' + service.name + '/status';
      self.HTTP.rest(statusCheckURL, function(f) {
        var status = f.status;

        var sMap = self.servicesMap;
        sMap[[app.name,service.name]].status = status;
        sMap[[app.name,service.name]].imgClass = status === 'RUNNING' ? 'complete' : 'loading';

      });

      var modified = false;
      if (sMap[[app.name,service.name]] == undefined) {
        sMap[[app.name,service.name]] = C.Service.create({
          metricEndpoint: C.Util.getMetricEndpoint(service.name),
          metricName: C.Util.getMetricName(service.name),
          runnablesList: [],
          runnablesMap: {},
          deleted: false,
          isValid: true,
        });
      }
      sMap[[app.name,service.name]].modelID = service.name;
      sMap[[app.name,service.name]].description = service.description;
      sMap[[app.name,service.name]].id = service.name;
      sMap[[app.name,service.name]].name = service.name;
      sMap[[app.name,service.name]].description = service.description;
      sMap[[app.name,service.name]].appID = service.app;
      sMap[[app.name,service.name]].isValid = true;
      sMap[[app.name,service.name]].deleted = false;

      for (var i=0; i < userServices.length; i++) {
        if (userServices[i].get('name') === service.name && userServices[i].get('appID') === app.name) {
          userServices[i].set('modelID', service.name);
          userServices[i].set('description', service.description);
          userServices[i].set('id', service.name);
          userServices[i].set('name', service.name);
          userServices[i].set('description', service.description);
          userServices[i].set('metricEndpoint', C.Util.getMetricEndpoint(service.name));
          userServices[i].set('metricName', C.Util.getMetricName(service.name));
          userServices[i].set('appID', service.app);
          userServices[i].set('isValid', true);
          userServices[i].set('deleted', false);
          modified = true;
        }
      }

      if (!modified) {
        userServices.push(C.Service.create({
          modelId: service.name,
          description: service.description,
          id: service.name,
          name: service.name,
          description: service.description,
          metricEndpoint: C.Util.getMetricEndpoint(service.name),
          metricName: C.Util.getMetricName(service.name),
          appID: service.app,
          runnablesList: [],
          runnablesMap: {},
          status: 'STOPPED', //default status and imgClass values:
          imgClass: 'loading',
          isValid: true,
        }));
      }
      userServices.arrayContentDidChange();
    },

    updateApp: function (app) {
      var self = this;
      var appUrl = 'apps/' + app.name + '/services';
      self.HTTP.rest(appUrl, function (services) {
        services.forEach(function(service) {
          self.updateService(app, service);
        });
      });
    },

    resetUserServices: function () {
      var self = this;
      /*
      var userServices = self.get('userServices');

      for (var i=0; i < userServices.length; i++) {
        if(userServices[i].get('isValid') == false && userServices[i].deleted == false) {
          userServices[i].set('deleted', true);
//          userServices.splice(i, 1);
//          userServices.arrayContentDidChange();
          continue;
        }
        userServices[i].set('isValid', false);
      }
*/

      self.HTTP.rest('apps', function (apps) {
//        console.log("numapps: " + apps.length);
        apps.forEach(function(app) {
          self.updateApp(app);
        });

        // Bind all the tooltips after UI has rendered after call has returned.
        setTimeout(function () {
          $("[data-toggle='tooltip']").off()
          $("[data-toggle='tooltip']").tooltip();
        }, 1000);
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

      self.resetUserServices();
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

    //TODO:
    getRuntimeArgs: function (appID, serviceID) {
      self.HTTP.rest('/apps/' + appID + '/services/' + serviceID + '/runtimeargs');
      return;
    },

    //TODO: create increase/decrease-Instance functions for runnables. The following are for system services.
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
