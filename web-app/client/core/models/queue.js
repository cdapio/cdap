/*
 * Flowlet Model
 */

define(['core/models/element'], function (Element) {

  var Model = Element.extend({

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
        .replace(/\{id\}/, this.get('id').replace('_IN', '').replace('_OUT', ''));

    }

  });

  Model.reopenClass({
    type: 'Queue',
    kind: 'Model'
  });

  return Model;

});