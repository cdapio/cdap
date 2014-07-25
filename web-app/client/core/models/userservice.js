/*
 * User Service Model
 */

define(['core/models/program'], function (Program) {

  var Model = Program.extend({
    type: 'Userservice',
    plural: 'services',
    href: function () {
      return '#/services/user/' + this.get('id');
    }.property('id'),

    init: function() {
      this._super();
      this.set('id', this.get('app') +  ":" + this.get('modelId'));
    },

    context: function () {
      // Check this.
      return 'user/services/' + this.get('id');
    }.property('id'),

    interpolate: function (path) {
      return path.replace(/\{id\}/, this.get('id')).replace(/\{app\}/, this.get('app'));
    },

    updateRunnable: function (runnable, index, http) {
      var self = this;
      var url = '/apps/' + self.app + '/services/' + self.name + '/runnables/' + runnable.id + '/instances';
      http.rest(url, function (runnablesResponse) {
        self.runnablesList[index].set('requested', runnablesResponse.requested);
        self.runnablesList[index].set('provisioned', runnablesResponse.provisioned);
      });
    },

    populateRunnablesAndUpdate : function (http, userServicesArray) {
      var self = this;
      http.rest('apps/' + self.app + '/services/' + self.name, function (serviceSpec) {
        var runnables = [];
        serviceSpec.runnables.forEach(function(runnable){
          runnables.push(Ember.Object.create({id : runnable}));
        });
        self.set('runnablesList', runnables);
        if(userServicesArray != undefined) {
          userServicesArray.pushObject(self);
        }

        self.update(http);
      });
    },

    update: function (http) {
      var self = this;

      self.runnablesList.forEach(function (runnable, index) {
        self.updateRunnable(runnable, index, http);
      });

      var url = '/apps/' + self.app + '/services/' + self.name + '/status';
      http.rest(url, function (statusResponse) {
        self.set('status', statusResponse.status);
        self.set('imgClass', statusResponse.status === 'RUNNING' ? 'complete' : 'loading');
      });
    },

    running: function () {
      return this.status === "RUNNING";
    }.property('status')

  });

  Model.reopenClass({
    type: 'Userservice',
    kind: 'Model',
    find: function(model_id, http) {
      var self = this;
      var promise = Ember.Deferred.create();

      var mid = model_id.split(':');
      var app_id = mid[0];
      var flow_id = mid[1];

      http.rest('apps', app_id, 'services', flow_id, function (model, error) {
        if (error !== 200) {
          promise.resolve(null);
        }
        model = C.Userservice.create(model);
        model.set('modelId', mid);
        model.set('id', model_id);
        model.set('app', app_id);
        model.populateRunnablesAndUpdate(http);
        promise.resolve(model);
      });

      return promise;

    }
  });

  return Model;

});
