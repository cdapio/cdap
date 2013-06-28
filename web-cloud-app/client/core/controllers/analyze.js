/*
 * Analyze Controller
 */

define([], function (chartHelper) {

  var DEFAULT_FORMAT = 'rate';
  var DEFAULT_DURATION = '7';

  var Controller = Em.Controller.extend(Em.Evented, {
    typesBinding: 'model.types',

    load: function (id) {
      this.set('format', DEFAULT_FORMAT);
      this.set('duration', +DEFAULT_DURATION);
      this.set('data', []);
      
      //Initial load with rate.
      this.changeMetricFormat(this.get('format'));
    },

    changeMetricFormat: function(formatType) {
      var self = this;
      var duration = this.get('duration')
      C.HTTP.get('metrics/events_in?format='+formatType+'&duration='+duration,
        function(status, result) {
          self.set('data', result);
          self.set('format', formatType);
        }
      );
    },

    changeMetricDuration: function(duration) {
      var self = this;
      var format = this.get('format');
      C.HTTP.get('metrics/events_in?format='+format+'&duration='+duration,
        function(status, result) {
          self.set('data', result);
          self.set('duration', +duration)
        }
      );
    },

    unload: function () {

    },

    delete: function () {

    }

  });

  Controller.reopenClass({
    type: 'Analyze',
    kind: 'Controller'
  });

  return Controller;

});