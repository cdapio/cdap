/*
 * Flow Stream Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({

		/*
		 * This syntax makes the FlowStatus controller available to this controller.
		 * This allows us to access the flow model that has already been loaded.
		 */
		needs: ['FlowStatus'],

		load: function () {
			this.clearTriggers(true);
			var self = this;
			/*
			 * Give the chart Embeddables 100ms to configure
			 * themselves before updating.
			 */
			setTimeout(function () {
				self.getStats();
			}, C.EMBEDDABLE_DELAY);

		},

		unload: function () {
			clearTimeout(this.__timeout);
		},

		ajaxCompleted: function () {
			return this.get('timeseriesCompleted') && this.get('aggregatesCompleted');
		},

		clearTriggers: function (value) {
			this.set('timeseriesCompleted', value);
			this.set('aggregatesCompleted', value);
		},

		getStats: function () {
			if (!this.ajaxCompleted()) {
				return;
			}
			var models = [this.get('model')];
			this.clearTriggers(false);
			C.Util.updateAggregates(models, this.HTTP, this);
			C.Util.updateTimeSeries(models, this.HTTP, this);

			var self = this;
			self.__timeout = setTimeout(function () {
				self.getStats(self);
			}, C.POLLING_INTERVAL);

		},

		close: function () {

			var model = this.get('controllers').get('FlowStatus').get('model');

			/*
			 * HAX: The URL route needs the ID of a flow to be app_id:flow_id.
			 * However, Ember is smart enough to not reload the parent controller.
			 * Therefore, the "correct" ID is preserved on the parent controller's model.
			 */

			if (model.id && model.id.indexOf(':') === -1) {
				model.id = model.app + ':' + model.id;
			}

			this.transitionToRoute('FlowStatus', model);

		},

		injector: Ember.TextField.extend({
			valueBinding: 'parentView.injectValue',
			insertNewline: function() {
				var value = this.get('value');
				if (value) {
					this.get('parentView').inject();
				}
			}
		}),
		__timeout: null,
		injectValue: null,
		inject: function () {

			var payload = this.get('injectValue');
			var flow = this.get('controllers').get('FlowStatus').get('model');
			var stream = this.get('model').id;

			this.set('injectValue', '');

			this.HTTP.rpc('gateway', 'inject', {
				name: flow,
				stream: stream,
				payload: payload
			}, function (response, status) {

				if (response && response.error) {
					C.Modal.show(
					"Inject Error",
					"The gateway responded with: " + response.error.statusCode + '. Info: ' +
						JSON.stringify(response.error.response),
					true, true);
				}

			});
		}
	});

	Controller.reopenClass({

		type: 'FlowStatusStream',
		kind: 'Controller'

	});

	return Controller;

});