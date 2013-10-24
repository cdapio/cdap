/*
 * Flowlet Model
 */

define(['core/models/element'], function (Element) {

  var Model = Element.extend({

    type: 'Queue',
    plural: 'Queues',

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