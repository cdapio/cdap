/*
 * Dataset Model
 */

define(['core/models/element'], function (Element) {

	var Model = Element.extend({
		type: 'Dataset',
		plural: 'Datasets',
		href: function () {
			return '#/datasets/' + this.get('id');
		}.property(),
		init: function() {

			this._super();

			if (!this.get('id')) {
				this.set('id', this.get('name'));
			}

			this.trackMetric('/reactor/datasets/{id}/dataset.store.bytes', 'aggregates', 'storage');

		},

		interpolate: function (path) {

			return path.replace(/\{id\}/, this.get('id'));

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