/*
 * User Service Model
 */

define(['core/models/program'], function (Program) {

  var Model = Program.extend({
    type: 'UserService',
    plural: 'UserServices',
    href: function () {
      return '#/user/' + this.get('id');
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
      return path.replace(/\{id\}/, this.get('id'));
    }

  });

  Model.reopenClass({
    type: 'Userservice',
    kind: 'Model',
    find: function(model_id, http) {
      var self = this;
      var promise = Ember.Deferred.create();

      var model_id = model_id.split(':');
      var app_id = model_id[0];
      var flow_id = model_id[1];

      http.rest('apps', app_id, 'services', flow_id, function (model, error) {

        model = C.Userservice.create(model);
        model.set('modelId', model_id);
        model.set('app', app_id);
        promise.resolve(model);
      });

      return promise;

    }
  });

  return Model;

});
