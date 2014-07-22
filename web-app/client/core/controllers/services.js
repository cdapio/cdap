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

    config: function(appID, serviceID) {
			var self = this;
			var model = this.servicesMap[[appID, serviceID]];

			this.transitionToRoute('Service.Config', model);
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
      console.log('Updated View');
      return returnArray;
    }.property('userServices'),

    updateRunnable: function (app, service, runnable) {
      var self = this;
      var sMap = self.servicesMap;

      var runnableStatusURL = 'apps/' + app.name + '/services' + '/' + service.name + '/runnables' + '/' + runnable + '/instances';
      self.HTTP.rest(runnableStatusURL, function(f) {
        var map2 = sMap[[app.name,service.name]].get('runnablesMap');
        if(map2[runnable] == undefined) {
          map2[runnable] = Ember.Object.create();
        }

        map2[runnable].set('name', runnable);
        map2[runnable].set('requested', f.requested);
        map2[runnable].set('provisioned', f.provisioned);

        var list = [];
        var keys = Object.keys(map2);
        for (var m=0; m < keys.length; m++) {
          var val = map2[keys[m]];
          list.push(val);
        }
        sMap[[app.name,service.name]].set('runnablesList', list);
      });
    },


    updateService: function (app, service) {
      var self = this;
      var sMap = self.servicesMap;

      var runnableNameURL = 'apps/' + app.name + '/services' + '/' + service.name;
      self.HTTP.rest(runnableNameURL, function(response) {
        response.runnables.forEach( function(runnable) {
          self.updateRunnable(app, service, runnable);
        });
      });

      var statusCheckURL = 'apps/' + app.name + '/services' + '/' + service.name + '/status';
      self.HTTP.rest(statusCheckURL, function(response) {
        var status = response.status;
        var sMap = self.servicesMap;
        sMap[[app.name,service.name]].set('status', status);
        sMap[[app.name,service.name]].set('imgClass', status === 'RUNNING' ? 'complete' : 'loading');

      });

      if (sMap[[app.name,service.name]] == undefined) {
        sMap[[app.name,service.name]] = C.Service.create({
          metricEndpoint: C.Util.getMetricEndpoint(service.name),
          metricName: C.Util.getMetricName(service.name),
          runnablesList: [],
          runnablesMap: {},
          isValid: true,
          deleted: false,
        });
      }
      sMap[[app.name,service.name]].set('modelID', service.name);
      sMap[[app.name,service.name]].set('description', service.description);
      sMap[[app.name,service.name]].set('id', service.name);
      sMap[[app.name,service.name]].set('name', service.name);
      sMap[[app.name,service.name]].set('description', service.description);
      sMap[[app.name,service.name]].set('appID', service.app);
      sMap[[app.name,service.name]].set('app', service.app);
      sMap[[app.name,service.name]].set('isValid', true);
      sMap[[app.name,service.name]].set('deleted', false);

    },

    updateApp: function (app) {
      var self = this;
      var appUrl = 'apps/' + app.name + '/services';
      self.HTTP.rest(appUrl, function (services) {
        services.forEach(function(service) {
          self.set('userServices', []);
          self.get('userServices').pushObject(1); //This is needed, for some reason... I don't yet know why.
          self.updateService(app, service);
        });
      });
    },

    resetUserServices: function () {
      var self = this;

      var sMap = self.servicesMap;
      var keys = Object.keys(sMap);
      for (var m=0; m < keys.length; m++) {
        var obj = sMap[keys[m]];
        if (obj.isValid==false && obj.deleted==false) {
          obj.set('deleted', true);
          console.log('deleted: ' + obj.name);
        }
        obj.set('isValid', false);
      }

      self.HTTP.rest('apps', function (apps) {
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
