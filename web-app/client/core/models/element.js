/*
 * Base Element Model
 */

define([], function () {

  var Element = Em.Object.extend({

    units: {
      'events': 'number',
      'storage': 'bytes',
      'containers': 'number',
      'cores': 'number'
    },

    trackMetric: function (path, kind, label, interpolate) {

      path = this.interpolate(path);
      this.get(kind).set(C.Util.enc(path), Em.Object.create({
        path: path,
        value: label || [],
        interpolate: interpolate
      }));
      return path;

    },

    setMetric: function (label, value) {

      var unit = this.get('units')[label];
      value = C.Util[unit](value);

      this.set(label + 'Label', value[0]);
      this.set(label + 'Units', value[1]);

    },

    clearMetrics: function () {

      this.set('timeseries', Em.Object.create());
      this.set('aggregates', Em.Object.create());
      this.set('rates', Em.Object.create());

    },

    updateState: function (http, done) {

      if (!this.get('context')) {
        if (typeof done === 'function') {
          done(null);
        }
        return;
      }

      var self = this;

      http.rest(this.get('context').slice(1), 'status', function (response) {

        if (!$.isEmptyObject(response)) {
          self.set('currentState', response.status);
        }

        if (typeof done === 'function') {
          done(response.status);
        }

      });
    }

  });

  return Element;

});