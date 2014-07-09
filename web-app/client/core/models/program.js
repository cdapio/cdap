/*
 * Base Program Model
 */

define(['core/models/element'], function (Element) {

  var Program = Element.extend({

    running: function () {

      return this.get('currentState') === 'RUNNING' ? true : false;

    }.property('currentState').cacheable(false),
    started: function () {
      return this.lastStarted >= 0 ? $.timeago(this.lastStarted) : 'No Date';
    }.property('timeTrigger'),
    stopped: function () {
      return this.lastStopped >= 0 ? $.timeago(this.lastStopped) : 'No Date';
    }.property('timeTrigger'),
    actionIcon: function () {

      if (this.currentState === 'RUNNING' ||
        this.currentState === 'PAUSING') {
        return 'btn-stop';
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

    startDisabled: function () {
      if (this.currentState === 'RUNNING' ||
          this.currentState === 'STARTING' ||
          this.currentState === 'STOPPING') {
        return true;
      }
      return false;
    }.property('currentState'),
    startStopDisabled: function () {

      if (this.currentState !== 'STOPPED' &&
        this.currentState !== 'PAUSED' &&
        this.currentState !== 'DEPLOYED' &&
        this.currentState !== 'RUNNING') {
        return true;
      }
      return false;

    }.property('currentState'),
    defaultAction: function () {
      if (!this.currentState) {
        return '...';
      }
      return {
        'deployed': 'Start',
        'stopped': 'Start',
        'stopping': 'Start',
        'starting': 'Start',
        'running': 'Stop',
        'adjusting': '...',
        'draining': '...',
        'failed': 'Start'
      }[this.currentState.toLowerCase()];
    }.property('currentState'),

    start: function (http) {

      var model = this;
      http.rest(model.get('context'), 'runtimeargs', function (config) {
        model.startWithConfig(http, config);
      });

    },

    startWithConfig: function (http, config) {

      var model = this;
      model.set('currentState', 'STARTING');

      http.rpc(model.get('context'), 'start', {
        data: config
      }, function (response) {
        if (response.error) {
          C.Modal.show(response.error, response.message);
        } else {
          model.set('lastStarted', new Date().getTime() / 1000);
        }
      });

    },

    stop: function (http, done) {

      var model = this;
      model.set('currentState', 'STOPPING');

      http.rpc(model.get('context'), 'stop', function (response) {
        if (response.error) {
          C.Modal.show(response.error, response.message);
        }

        if (typeof done === 'function') {
          done(response);
        }
      });

    },

    updateState: function (http, done) {

      if (!this.get('context')) {
        if (typeof done === 'function') {
          done(null);
        }
        return;
      }

      var self = this;

      http.rest(this.get('context'), 'status', function (response) {

        if (!$.isEmptyObject(response)) {
          self.set('currentState', response.status);
        }

        if (typeof done === 'function') {
          done(response.status);
        }

      });
    },

    pluralInstances: function () {

      if (C.get('isLocal')) {
        return this.instances === 1 ? '' : 's';
      } else {
        return +this.containersLabel === 1 ? '' : 's';
      }

    }.property('instances', 'containersLabel')

  });

  return Program;

});