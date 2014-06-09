/*
 * Dataset Controller
 */

define([], function () {

	var Controller = Em.ArrayProxy.extend({

		elements: Em.Object.create(),

		load: function () {

			var self = this;
			var model = this.get("model");
			this.clearTriggers(true);

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

			this.HTTP.rest('datasets', this.get('model').id, 'flows',
				function (models, error) {

				var i = models.length;
				while (i--) {
					models[i] = C.Flow.create(models[i]);
				}
				self.set('elements.Flow', Em.ArrayProxy.create({content: models}));

			});

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
			var models = [this.get('model')].concat(this.get('elements.Flow.content'));
			this.clearTriggers(false);
      C.Util.updateTimeSeries(models, this.HTTP, this);
      C.Util.updateAggregates(models, this.HTTP, this);

		},

		unload: function () {

			this.set('types', Em.Object.create({
				'DatasetFlow': Em.ArrayProxy.create({
					content: []
				})
			}));

			clearInterval(this.interval);

		}

	});

	Controller.reopenClass({
		type: 'Dataset',
		kind: 'Controller'
	});

	return Controller;

});
