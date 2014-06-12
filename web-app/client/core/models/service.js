/*
 * Flow Model
 */

define([], function () {

  var Model = Em.Object.extend({

    init: function() {

      this._super();
      this.set('id', this.get('modelId'));
      this.set('description', this.get('meta') || 'Service');
    },

    /*
     * Runnable context path, used by user-defined metrics.
     */
    context: function () {
      return 'system/services/' + this.get('id');
    }.property('id')

  });

  Model.reopenClass({
    type: 'Service',
    kind: 'Model',
    find: function(model_id, http) {
      var self = this;

      var model = C.Service.create({
        modelId: model_id
      });

      return model;

    }

  });

  return Model;

});
