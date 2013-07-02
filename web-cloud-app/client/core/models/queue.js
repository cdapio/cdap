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

    trackMetric: function (name, type, label) {

      name = name.replace(/{parent}/, this.get('flowlet'));
      name = name.replace(/{id}/, this.get('id'));
      this.get(type)[name] = label;

      return name;

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