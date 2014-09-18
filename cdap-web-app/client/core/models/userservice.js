/*
 * User Service Model
 */

define(['core/models/program'], function (Program) {

  var Model = Program.extend({
    type: 'Userservice',
    plural: 'Userservices',
    href: function () {
      return '#/services/user/' + this.get('id');
    }.property('id'),

    appHref: function () {
      return '#/apps/' + this.get('app');
    }.observes('app').property('app'),

    init: function() {
      this._super();
      this.set('id', this.get('app') +  ":" + this.get('name'));
    },

		context: function () {
			return 'apps/' + this.app + '/services/' + this.name;
		}.property('app', 'name'),

    interpolate: function (path) {
      return path.replace(/\{id\}/, this.get('id')).replace(/\{app\}/, this.get('app'));
    },

    isRunning: function() {
      return this.get('currentState') === "RUNNING";
    }.property('currentState'),

    updateRunnable: function (runnable, http) {
      var self = this;
      var url = 'apps/' + self.app + '/services/' + self.name
          + '/runnables/' + runnable.id + '/instances';
      http.rest(url, function (runnablesResponse) {
        runnable.set('requested', runnablesResponse.requested);
        runnable.set('provisioned', runnablesResponse.provisioned);
      });
    },

    populateRunnablesAndUpdate : function (http, userServicesArray) {
      var self = this;
      http.rest('apps/' + self.app + '/services/' + self.name, function (serviceSpec) {
        var workers = [];
        var handler;
        serviceSpec.runnables.forEach(function(runnable){
          obj = Ember.Object.create({id : runnable});
          if(runnable === self.name) {
            handler = obj;
          } else {
            workers.push(obj);
          }
        });
        self.set('workersList', workers);
        self.set('handler', handler);
        self.set('numWorkers', workers.length);
        if(userServicesArray != undefined) {
          userServicesArray.pushObject(self);
        }

        self.update(http);
      });
    },

    update: function (http) {
      var self = this;

      self.workersList.forEach(function (worker) {
        self.updateRunnable(worker, http);
      });
      self.updateRunnable(self.handler, http);

      var url = 'apps/' + self.app + '/services/' + self.name + '/status';
      http.rest(url, function (statusResponse) {
        self.set('currentState', statusResponse.status);
        self.set('status', statusResponse.status);
        self.set('imgClass', statusResponse.status === 'RUNNING' ? 'complete' : 'loading');
      });
    },

  });

  Model.reopenClass({
    type: 'Userservice',
    kind: 'Model',
    find: function(model_id, http) {
      var self = this;
      var promise = Ember.Deferred.create();

      var mid = model_id.split(':');
      var app_id = mid[0];
      var service_id = mid[1];

      http.rest('apps', app_id, 'services', service_id, function (model, error) {
        if (error !== 200) {
          promise.resolve(null);
        }
        model.app = app_id;
        model = C.Userservice.create(model);
        model.populateRunnablesAndUpdate(http);
        promise.resolve(model);
      });

      return promise;

    }
  });

  return Model;

});
