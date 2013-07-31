/*
 * Dataset Model
 */

define([], function () {

	var Model = Em.Object.extend({
		type: 'Dataset',
		plural: 'Datasets',
		href: function () {
			return '#/datasets/' + this.get('id');
		}.property(),
		init: function() {

			this._super();
			this.set('timeseries', Em.Object.create());
			this.set('aggregates', Em.Object.create());
			if (!this.get('id')) {
				this.set('id', this.get('name'));
			}

			this.trackMetric('/store/bytes/datasets/{id}', 'aggregates', 'storage');

		},

		units: {
			'storage': 'bytes',
			'events': 'number'
		},

		interpolate: function (path) {

			return path.replace(/\{id\}/, this.get('id'));

		},

		trackMetric: function (path, kind, label) {

			this.get(kind).set(path = this.interpolate(path), label || []);
			return path;

		},

		setMetric: function (label, value) {

			var unit = this.get('units')[label];
			value = C.Util[unit](value);

			this.set(label + 'Label', value[0]);
			this.set(label + 'Units', value[1]);

		}

	});

	Model.reopenClass({
		type: 'Dataset',
		kind: 'Model',
		find: function (dataset_id, http) {
			var promise = Ember.Deferred.create();

			http.rest('datasets', dataset_id, function (model, error) {

				model = C.Dataset.create(model);
				promise.resolve(model);

			});

			return promise;
		}
	});

	return Model;

});