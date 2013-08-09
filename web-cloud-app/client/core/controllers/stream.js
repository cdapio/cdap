/*
 * Stream Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),

		load: function (id) {

			var self = this;

			this.interval = setInterval(function () {
				self.updateStats();
			}, C.POLLING_INTERVAL);

			/*
			 * Give the chart Embeddables 100ms to configure
			 * themselves before updating.
			 */
			setTimeout(function () {
				self.updateStats();
			}, C.EMBEDDABLE_DELAY);

			this.HTTP.rest('streams', this.get('model').id, 'flows', function (models, error) {

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

			clearInterval(this.interval);
			this.set('elements', Em.Object.create());

		},

		"delete": function () {

			C.Modal.show("Delete Stream",
				"Are you sure you would like to delete this Stream? This action is not reversible.",
				$.proxy(function (event) {

					var stream = this.get('model');

					C.get('metadata', {
						method: 'deleteStream',
						params: ['Stream', {
							id: stream.id
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
		type: 'Stream',
		kind: 'Controller'
	});

	return Controller;

});