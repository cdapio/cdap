/*
 * Dataset Controller
 */

define([], function () {

	var Controller = Em.ArrayProxy.extend({

		elements: Em.Object.create(),

		load: function () {

			var self = this;
			var model = this.get("model");

			self.interval = setInterval(function () {
				self.updateStats();
			}, C.POLLING_INTERVAL);
			/*
			 * Give the chart Embeddables 100ms to configure
			 * themselves before updating.
			 */
			setTimeout(function () {
				self.updateStats();
			}, C.EMBEDDABLE_DELAY);

			this.HTTP.rest('datasets', this.get('model').id, 'flows', function (models, error) {

				var i = models.length;
				while (i--) {
					models[i] = C.Flow.create(models[i]);
				}
				self.set('elements.Flow', Em.ArrayProxy.create({content: models}));

			});

		},

		updateStats: function () {

			var models = [this.get('model')].concat(this.get('elements.Flow.content'));

      C.Util.updateTimeSeries(models, this.HTTP);
      C.Util.updateAggregates(models, this.HTTP);

		},

		unload: function () {

			this.set('types', Em.Object.create({
				'DatasetFlow': Em.ArrayProxy.create({
					content: []
				})
			}));

			clearInterval(this.interval);

		},

		"delete": function () {

			C.Modal.show(
				"Delete Dataset",
				"Are you sure you would like to delete this Dataset? This action is not reversible.",
				$.proxy(function (event) {

					var model = this.get('model');

					C.get('metadata', {
						method: 'deleteDataset',
						params: ['Dataset', {
							id: model.id
						}]
					}, function (error, response) {

						if (error) {
							C.Modal.show('Delete Error', error.message);
						} else {
							window.history.go(-1);
						}

					});
				}, this));

		}

	});

	Controller.reopenClass({
		type: 'Dataset',
		kind: 'Controller'
	});

	return Controller;

});
