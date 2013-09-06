/*
 * Procedure Controller
 */

define([], function () {

	var Controller = Ember.Controller.extend({

		load: function () {
			this.clearTriggers(true);
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

		ajaxCompleted: function () {
			return this.get('timeseriesCompleted') && this.get('aggregatesCompleted');
		},

		clearTriggers: function (value) {
			this.set('timeseriesCompleted', value);
			this.set('aggregatesCompleted', value);
		},

		updateStats: function () {
			if (!this.ajaxCompleted()) {
				return;
			}
			this.get('model').updateState(this.HTTP);
			this.clearTriggers(false);
			C.Util.updateTimeSeries([this.get('model')], this.HTTP, this);
			C.Util.updateAggregates([this.get('model')], this.HTTP, this);

		},
		/**
		 * Lifecycle
		 */

		start: function (app, id, version, config) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STARTING');

			this.HTTP.rpc('runnable', 'start', [app, id, version, 'QUERY', config],
				function (response) {

					if (response.error) {
						C.Modal.show(response.error.name, response.error.message);
					} else {
						model.set('lastStarted', new Date().getTime() / 1000);
					}

			});

		},
		stop: function (app, id, version) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STOPPING');

			this.HTTP.rpc('runnable', 'stop', [app, id, version, 'QUERY'],
				function (response) {

					if (response.error) {
						C.Modal.show(response.error.name, response.error.message);
					}

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

		'delete': function () {

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