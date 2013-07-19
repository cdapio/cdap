/*
 * Flowlet Model
 */

define([], function () {

  var Model = Em.Object.extend({

    type: 'Queue',
    plural: 'Queues',

    init: function() {
      this._super();

      this.set('timeseries', Em.Object.create());
      this.set('aggregates', Em.Object.create());

    },

    interpolate: function (path) {

      return path.replace(/\{app\}/, this.get('app'))
        .replace(/\{flow\}/, this.get('flow'))
        .replace(/\{flowlet\}/, this.get('flowlet'))
        .replace(/\{id\}/, this.get('id'));

    },

    trackMetric: function (path, kind, label) {

      this.get(kind).set(path = this.interpolate(path), label || []);
      return path;

    },

    units: {
      'events': 'number'
    },

    setMetric: function (label, value) {

      var unit = this.get('units')[label];
      value = C.Util[unit](value);

      this.set(label + 'Label', value[0]);
      this.set(label + 'Units', value[1]);

    }

  });

  Model.reopenClass({
    type: 'Queue',
    kind: 'Model'
  });

  return Model;

});