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

      self.set('map', {});
      self.resetServices();
      this.interval = setInterval(function () {
        self.resetServices();
      }, C.POLLING_INTERVAL)

    },

    servicesArray:  function () {
      var self = this;
      var returnArray = [];
      var input = self.get('userServices');
      var keys = Object.keys(input);
      //the last key is "super" or something...
      for (var i=0; i < keys.length-1; i++) {
        var key = keys[i];
//        if(typeof key == 'string' || key instanceof String){
          var obj = input[key];
          returnArray.push(obj);
//        }
      }
      return returnArray;
    }.property('userServices'),

    updateRunnable: function (app, service, runnable) {
      var self = this;
      var userServices = self.get('userServices');
      var runnableStatusURL = 'apps/' + app.name + '/services' + '/' + service.name + '/runnables' + '/' + runnable + '/instances';
      self.HTTP.rest(runnableStatusURL, function(f) {
        for (var i=0; i < userServices.length; i++) {
          if  (userServices[i].name === service.name && userServices[i].appID === app.name) {
            var list = userServices[i].get('runnablesList');
            var map = userServices[i].runnablesMap;
//            if(map[runnable] == undefined) {
//              map[runnable] = Ember.Object.create({});
//            }
//            map[runnable].set('name', runnable);
//            map[runnable].set('requested', f.requested);
//            map[runnable].set('provisioned', f.provisioned);

            if(map[runnable] == undefined) {
              map[runnable] = {};
            }
            map[runnable].name = runnable;
            map[runnable].requested = f.requested;
            map[runnable].provisioned = f.provisioned;

            list.clear();
            var keys = Object.keys(map);
            for (var m=0; m < keys.length; m++) {
             var val = map[keys[m]];
             list.pushObject(val);
            }
            userServices[i].set('runnablesList', list);
            userServices[i].runnablesMap = map;
            list.arrayContentDidChange();
            userServices.arrayContentDidChange();
          }
        }
      });
    },

    updateService: function (app, service) {
      var self = this;
      var userServices = self.get('userServices');
      var runnableNameURL = 'apps/' + app.name + '/services' + '/' + service.name;
      self.HTTP.rest(runnableNameURL, function(f) {
        f.runnables.forEach( function(runnable) {
          self.updateRunnable(app, service, runnable);
        });
      });

      var statusCheckURL = 'apps/' + app.name + '/services' + '/' + service.name + '/status';
      self.HTTP.rest(statusCheckURL, function(f) {
        var status = f.status;
        for (var i=0; i < userServices.length; i++) {
          if  (userServices[i].get('name') === service.name && userServices[i].get('appID') === app.name) {
            userServices[i].set('status', status);
            userServices[i].set('imgClass', status === 'RUNNING' ? 'complete' : 'loading');
            userServices.arrayContentDidChange();
          }
        }
      });

      var modified = false;
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
        console.log('pushed: ' + service.name);
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
          deleted: false,
        }));
      }
      userServices.arrayContentDidChange();
    },

    updateApp: function (app) {
      var self = this;
      var userServices = self.get('userServices');
      var appUrl = 'apps/' + app.name + '/services';
      self.HTTP.rest(appUrl, function (services) {
        services.forEach(function(service) {
          self.updateService(app, service);
        });
      });
    },

    resetUserServices: function () {
      var self = this;
      var userServices = self.get('userServices');

      for (var i=0; i < userServices.length; i++) {
        if(userServices[i].get('isValid') == false && userServices[i].deleted == false) {
          userServices[i].set('deleted', true);
//          userServices.splice(i, 1);
//          userServices.arrayContentDidChange();
          console.log('deleting' + userServices[i].name);
          continue;
        }
        userServices[i].set('isValid', false);
      }
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
