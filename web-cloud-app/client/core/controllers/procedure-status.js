/*
 * Procedure Controller
 */

define([], function () {

	var Controller = Ember.Controller.extend({

		load: function () {

			var model = this.get('model');
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

		unload: function () {

			clearInterval(this.interval);

		},

		updateStats: function () {

			C.get.apply(C, this.get('model').getUpdateRequest());

		},

		start: function (app, id, version, config) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STARTING');

			C.get('manager', {
				method: 'start',
				params: [app, id, -1, 'QUERY', config]
			}, function (error, response) {

				model.set('lastStarted', new Date().getTime() / 1000);

			});

		},
		stop: function (app, id, version) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STOPPING');

			C.get('manager', {
				method: 'stop',
				params: [app, id, -1, 'QUERY']
			}, function (error, response) {


			});

		},

		/**
		 * Action handlers from the View
		 */

		responseBody: null,
		responseCode: null,

		submit: function (event) {

			var self = this;
			C.get('gateway', {
				method: 'query',
				params: {
					service: this.get('model').serviceName,
					app: this.get('model').applicationId,
					method: this.get('requestMethod'),
					payload: this.get('requestParams')
				}
			}, function (error, response) {

				if (error) {
					self.set('responseCode', error.statusCode);
					self.set('responseBody', JSON.stringify(error.response, undefined, 2) || '[ No Content ]');
				} else {
					self.set('responseCode', response.statusCode);
					var pretty;
					try {
						pretty = JSON.stringify(JSON.parse(response.params.response), undefined, 2);
					} catch (e) {
						pretty = response.params.response;
					}
					self.set('responseBody', pretty || '[ No Content ]');
				}

			});

		},

		exec: function (action) {

			var control = $(event.target);
			if (event.target.tagName === "SPAN") {
				control = control.parent();
			}

			var id = control.attr('flow-id');
			var app = control.attr('flow-app');
			var action = control.attr('flow-action');

			if (action && action.toLowerCase() in this) {
				this[action.toLowerCase()](app, id, -1);
			}
		},

		"delete": function () {

			if (this.get('model').get('currentState') !== 'STOPPED' &&
				this.get('model').get('currentState') !== 'DEPLOYED') {
				C.Modal.show('Cannot Delete', 'Please stop the Procedure before deleting.');
			} else {
				C.Modal.show(
					"Delete Procedure",
					"You are about to remove a Procedure, which is irreversible. You can upload this Procedure again if you'd like. Do you want to proceed?",
					$.proxy(function (event) {

						var procedure = this.get('model');

						C.get('far', {
							method: 'remove',
							params: [procedure.app, procedure.name, procedure.version, 'QUERY']
						}, function (error, response) {

							C.Modal.hide(function () {

								if (error) {
									C.Modal.show('Delete Error', error.message || 'No reason given. Please check the logs.');
								} else {
									window.history.go(-1);
								}

							});

						});
					}, this));
			}
		}

	});

	Controller.reopenClass({
		type: 'ProcedureStatus',
		kind: 'Controller'
	});

	return Controller;

});