/*
 * Spark Model
 */

define(['core/models/program'], function (Program) {

	var Model = Program.extend({
	  type: 'Spark',
    plural: 'Spark',
    href: function () {
    	return '#/spark/' + this.get('id');
    }.property('id'),

    currentState: '',

    init: function() {

    	this._super();

    	this.set('id', this.get('app') + ':' + this.get('name'));
    	this.set('description', this.get('meta') || 'Spark');

    },

    context: function () {

    	return this.interpolate('apps/{parent}/spark/{id}');

    }.property('app', 'name'),

    interpolate: function (path) {

    	return path.replace(/\{parent\}/, this.get('app'))
    		.replace(/\{id\}/, this.get('name'));

    },

    startStopDisabled: function () {

      if (this.currentState !== 'STOPPED') {
        return true;
      }
      return false;

    }.property('currentState')
	});

  Model.reopenClass({
    type: 'Spark',
    kind: 'Model',
    find: function(model_id, http) {

      var self = this,
        promise = Ember.Deferred.create(),

        model_id = model_id.split(':'),
        app_id = model_id[0],
        spark_id = model_id[1];

      http.rest('apps', app_id, 'spark', spark_id, function (model, error) {

      		http.rest('apps', app_id, 'spark', spark_id, 'status', function (response) {
      			if (!$.isEmptyObject(response)) {
      				model.set('currentState', response.status);
      				promise.resolve(model);
      			} else {
      				promise.reject('Status not found');
      			}
      		});

      });

      return promise;
    }
  });

  return Model;

});