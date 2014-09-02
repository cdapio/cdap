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

    	this.set('name', (this.get('sparkId') || this.get('id') || this.get('meta').name));

    	this.set('app', this.get('applicationId') || this.get('app') || this.get('meta').app);
    	this.set('id', this.get('app') + ':' +
    	  (this.get('sparkId') || this.get('id') || this.get('meta').name));

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
    find: function(spark_id, http) {

      var self = this,
        promise = Ember.Deferred.create(),

        model_id = model_id.split(':'),
        app_id = model_id[0],
        flow_id = model_id[1];

      http.rest('apps', app_id, 'spark', flow_id, function (model, error) {

      		http.rest('apps', app_id, 'spark', flow_id, 'status', function (response) {
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