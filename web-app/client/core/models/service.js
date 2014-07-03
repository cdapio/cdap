/*
 * Service Model
 */

define(['core/models/element'], function (Element) {

  var Model = Element.extend({
    type: 'Service',
    plural: 'Services',

    init: function() {
      this._super();
      this.set('id', this.get('modelId'));
    },
    
    context: function () {
      return 'system/services/' + this.get('id');
    }.property('id'),

    interpolate: function (path) {
      return path.replace(/\{id\}/, this.get('id'));
    }

  });

  Model.reopenClass({
    type: 'Service',
    kind: 'Model',
    find: function(model_id, http) {
      var self = this;

      var model = C.Service.create({
        modelId: model_id,
        metricEndpoint: C.Util.getMetricEndpoint(model_id),
        metricName: C.Util.getMetricName(model_id)
      });

      return model;

    }

  });

  return Model;

});
