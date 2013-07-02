/*
 * App Model
 */

define([], function () {

	var Model = Em.Object.extend({
		type: 'App',
		plural: 'Apps',

		href: function () {
			return '#/apps/' + this.get('id');
		}.property(),

		init: function() {
			this._super();

			this.set('timeseries', Em.Object.create());
			this.set('aggregates', Em.Object.create());

			this.set('counts', {
				Stream: 0,
				Flow: 0,
				Batch: 0,
				Dataset: 0,
				Query: 0
			});

		},

		units: {
			'storage': 'bytes'
		},

		trackMetric: function (name, type, label) {

			name = name.replace(/{parent}/, this.get('app'));
			name = name.replace(/{id}/, this.get('name'));

			this.get(type)[name] = label;

			return name;

		},

		setMetric: function (label, value) {

			var unit = this.get('units')[label];
			value = C.Util[unit](value);

			this.set(label + 'Label', value[0]);
			this.set(label + 'Units', value[1])

		}

	});

	Model.reopenClass({
		type: 'App',
		kind: 'Model',
		find: function(model_id, http) {

			var promise = Ember.Deferred.create();

			http.rest('apps', model_id, function (model, error) {

				model = C.App.create(model);
				promise.resolve(model);

			});

			return promise;
		}
	});

	return Model;

});