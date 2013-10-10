/*
 * Workflow Model
 */

define(['core/models/program'], function (Program) {

  var Model = Program.extend({
    type: 'Workflow',
    plural: 'Workflows',
    href: function () {
      return '#/workflows/' + this.get('id');
    }.property('id'),

    startTime: null,
    metricNames: null,
    instances: 0,

    init: function() {
      this._super();
      this.set('app', this.get('applicationId') || this.get('app') || this.get('appId'));
      this.set('id', this.get('app') + ':' + this.get('name'));
      this.set('nextRuns', []);
    },

    /*
     * Runnable context path, used by user-defined metrics.
     */
    context: function () {

      return this.interpolate('/apps/{parent}/workflows/{id}');

    }.property('app', 'name'),

    interpolate: function (path) {

      return path.replace(/\{parent\}/, this.get('app'))
        .replace(/\{id\}/, this.get('name'));

    },

    startStopDisabled: function () {

      if (this.currentState !== 'STOPPED') {
        return true;
      }
      return false;

    }.property('currentState')

  });

  Model.reopenClass({
    type: 'Workflow',
    kind: 'Model',
    find: function(model_id, http) {
      var self = this;
      var promise = Ember.Deferred.create();

      var model_id = model_id.split(':');
      var app_id = model_id[0];
      var workflowId = model_id[1];

      http.rest('apps', app_id, 'workflows', workflowId, function (model, error) {
        model.app = app_id;
        model = C.Workflow.create(model);

        http.rest('apps', app_id, 'workflows', workflowId, 'status', function (response) {

          if ($.isEmptyObject(response)) {
            promise.reject('Status could not retrieved.');
          } else {
            model.set('currentState', response.status);
            promise.resolve(model);
          }

        });

      });

      return promise;

    }
  });

  return Model;

});
