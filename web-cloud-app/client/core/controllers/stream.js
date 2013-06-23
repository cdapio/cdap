/*
 * Stream Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({
		typesBinding: 'model.types',

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

		},

		updateStats: function () {
			var self = this;

			// Update timeseries data for current flow.
			C.get.apply(C, this.get('model').getUpdateRequest());

		},

		unload: function () {

			clearInterval(this.interval);

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