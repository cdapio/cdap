/*
 * App Model
 */

define(['core/models/element'], function (Element) {

	var Model = Element.extend({
		type: 'App',
		plural: 'Apps',
		href: function () {
			return '#/apps/' + this.get('id');
		}.property(),

		init: function() {
			this._super();

			this.set('counts', {
				Stream: 0,
				Flow: 0,
				Mapreduce: 0,
				Dataset: 0,
				Query: 0
			});

		},

		units: {
			'storage': 'bytes',
			'containers': 'number',
			'cores': 'number'
		},

		/*
		 * Runnable context path, used by user-defined metrics.
		 */
		context: function () {

			return this.interpolate('/apps/{id}');

		}.property('id'),

		interpolate: function (path) {

			return path.replace(/\{id\}/, this.get('id'));

		},

		getSubPrograms: function (callback, http) {

			var types = ['flows', 'mapreduce', 'procedures'];
			var remaining = types.length, i = types.length;
			var result = {};
			var id = this.get('id');
			var kinds = {
				'flows': 'Flow',
				'mapreduce': 'Mapreduce',
				'procedures': 'Procedure'
			};

			while (i--) {

				(function () {

					var type = types[i];

					http.rest('apps', id, type, function (models) {

						var j = models.length;
						while (j--) {
							models[j] = C[kinds[type]].create(models[j]);
						}

						result[kinds[type]] = models;

						if (!--remaining) {
							callback(result);
						}

					});

				})();

			}

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
