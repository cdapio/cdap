/*
 * Procedure Controller
 */

define([], function () {

	var Controller = Ember.Controller.extend({

		load: function () {
			this.clearTriggers(true);
			var model = this.get('model');
			var self = this;

			this.set('requestMethod', '');
			this.set('requestParams', '');
			this.set('responseBody', '');

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
		 * Action handlers from the View
		 */
		exec: function () {

			var model = this.get('model');
			var action = model.get('defaultAction');
			if (action && action.toLowerCase() in model) {
				model[action.toLowerCase()](this.HTTP);
			}

		},

		config: function () {

			var self = this;
			var model = this.get('model');

			this.transitionToRoute('ProcedureStatus.Config');

		},

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

		}

	});

	Controller.reopenClass({
		type: 'ProcedureStatus',
		kind: 'Controller'
	});

	return Controller;

});