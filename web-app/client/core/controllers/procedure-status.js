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

		start: function (appId, id, version, config) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STARTING');

			this.HTTP.post('rest', 'apps', appId, 'procedures', id, 'start',
				function (response) {

					if (response.error) {
						C.Modal.show(response.error.name, response.error.message);
					} else {
						model.set('lastStarted', new Date().getTime() / 1000);
					}

			});

		},
		stop: function (appId, id, version) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STOPPING');

			this.HTTP.post('rest', 'apps', appId, 'procedures', id, 'stop',
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
			var appId = this.get('model').applicationId;
			var procedureName = this.get('model').serviceName;
			var methodName = this.get('requestMethod');

			this.HTTP.post('rest', 'apps', appId, 'procedures', procedureName, 'methods', methodName, {
					data: this.get('requestParams')
				}, function (response) {

				if (response) {
					if (typeof(response) === 'object') {
						self.set('responseBody', JSON.stringify(response, undefined, 2) || '[ No content ]');
					} else {
						self.set('responseBody', response || '[ No content ]');
					}

				} else {
					self.set('responseBody', '[ No response recevied ]');
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
		}

	});

	Controller.reopenClass({
		type: 'ProcedureStatus',
		kind: 'Controller'
	});

	return Controller;

});