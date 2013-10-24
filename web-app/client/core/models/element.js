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

    init: function () {

      this._super();

      this.set('timeseries', Em.Object.create());
      this.set('aggregates', Em.Object.create());
      this.set('currents', Em.Object.create());
      this.set('rates', Em.Object.create());

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

      this.set(label + 'Raw', value);

      var unit = this.get('units')[label];
      value = C.Util[unit](value);

      this.set(label + 'Label', value[0]);
      this.set(label + 'Units', value[1]);

    },

    clearMetrics: function () {

      this.set('timeseries', Em.Object.create());
      this.set('aggregates', Em.Object.create());
      this.set('rates', Em.Object.create());

    }

  });

  return Element;

});