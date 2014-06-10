/*
 * Procedure Model
 */

define(['core/models/program'], function (Program) {

	var Model = Program.extend({
		type: 'Procedure',
		plural: 'Procedures',
		href: function () {
			return '#/procedures/' + this.get('id');
		}.property('id'),

		instances: 0,
		version: -1,
		currentState: '',

		init: function() {
			this._super();

			this.set('name', (this.get('flowId') || this.get('id') || this.name));

			this.set('app', this.get('applicationId') || this.get('app'));
			this.set('id', this.get('app') + ':' +
				(this.get('flowId') || this.get('id') || this.name));

			this.set('description', 'Procedure');

		},

		/*
		 * Runnable context path, used by user-defined metrics.
		 */
		context: function () {

			return this.interpolate('/apps/{parent}/procedures/{id}');

		}.property('app', 'name'),

		interpolate: function (path) {

			return path.replace(/\{parent\}/, this.get('app'))
				.replace(/\{id\}/, this.get('name'));

		},

		getMeta: function () {
			var arr = [];
			for (var m in this.meta) {
				arr.push({
					k: m,
					v: this.meta[m]
				});
			}
			return arr;
		}.property('meta')

	});

	Model.reopenClass({
		type: 'Procedure',
		kind: 'Model',
		find: function(model_id, http) {
			var self = this;
			var promise = Ember.Deferred.create();

			var model_id = model_id.split(':');
			var app_id = model_id[0];
			var procedure_id = model_id[1];

			http.rest('apps', app_id, 'procedures', procedure_id, function (model, error) {
				var model = self.transformModel(model);
				model.applicationId = app_id;
				model = C.Procedure.create(model);

				http.rest('apps', app_id, 'procedures', procedure_id, 'status',
					function (response) {

						if (!$.isEmptyObject(response)) {
							model.set('currentState', response.status);
							promise.resolve(model);
						}

				});

			});

			return promise;

		},

		transformModel: function (model) {
			return {
				id: model.name,
				name: model.name,
				description: model.description,
				serviceName: model.name,
				datasets: model.datasets
			};
		}
	});

	return Model;

});
