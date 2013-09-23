/*
 * Workflow Model
 */

define([], function () {

  var Model = Em.Object.extend({

    href: function () {
      return '#/workflows/' + this.get('id');
    }.property('id'),

    metricNames: null,
    instances: 0,
    type: 'Workflow',
    plural: 'Workflows',
    startTime: null,

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

    started: function () {
      return this.lastStarted >= 0 ? $.timeago(this.lastStarted) : 'No Date';
    }.property('timeTrigger'),

    stopped: function () {
      return this.lastStopped >= 0 ? $.timeago(this.lastStopped) : 'No Date';
    }.property('timeTrigger'),

    actionIcon: function () {

      if (this.currentState === 'RUNNING' ||
        this.currentState === 'PAUSING') {
        return 'btn-pause';
      } else {
        return 'btn-start';
      }

    }.property('currentState').cacheable(false),

    stopDisabled: function () {

      if (this.currentState === 'RUNNING') {
        return false;
      }
      return true;

    }.property('currentState'),

    startPauseDisabled: function () {

      if (this.currentState !== 'STOPPED' &&
        this.currentState !== 'PAUSED' &&
        this.currentState !== 'DEPLOYED' &&
        this.currentState !== 'RUNNING') {
        return true;
      }
      return false;

    }.property('currentState'),

    updateState: function (http, opt_callback) {

      var self = this;

      var app_id = this.get('app'),
        workflowId = this.get('name');

      http.rest('apps', app_id, 'workflows', workflowId, 'status', function (response) {

        if (!$.isEmptyObject(response)) {
          self.set('currentState', response.status);
        }

        if (typeof opt_callback === 'function') {
          opt_callback();
        }

      });
    },

    defaultAction: function () {

      if (!this.currentState) {
        return '...';
      }

      return {
        'deployed': 'Start',
        'stopped': 'Start',
        'stopping': 'Start',
        'starting': 'Start',
        'running': 'Pause',
        'adjusting': '...',
        'draining': '...',
        'failed': 'Start'
      }[this.currentState.toLowerCase()];
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
