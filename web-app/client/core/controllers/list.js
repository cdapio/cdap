/*
 * List Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),

		__titles: {
			'App': 'Applications',
			'Flow': 'Process',
			'Mapreduce': 'Process',
			'Workflow': 'Process',
			'Stream': 'Collect',
			'Procedure': 'Query',
			'Dataset': 'Store'
		},

		__plurals: {
			'App': 'apps',
			'Flow': 'flows',
			'Mapreduce': 'mapreduce',
			'Workflow': 'workflows',
			'Stream': 'streams',
			'Procedure': 'procedures',
			'Dataset': 'datasets'
		},

		entityTypes: new Em.Set(),

		title: function () {
			return this.__titles[this.get('entityType')];
		}.property('entityType'),

		load: function (type) {

			this.clearTriggers(true);
			var self = this;
			this.set('entityType', type);

			this.entityTypes.add(type);

			this.HTTP.rest(this.__plurals[type], function (objects) {

				var i = objects.length;
				while (i--) {
					objects[i] = C[type].create(objects[i]);
				}

				self.set('elements.' + type, Em.ArrayProxy.create({content: objects}));

				clearInterval(self.interval);
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
			var content, self = this, models = [];
			for (var j=0; j<this.entityTypes.length; j++) {
				var objects = this.get('elements.' + this.entityTypes[j]);
				if (objects) {
					models = models.concat(objects.content);
				}
			}

			/*
			 * Hax until we have a pub/sub system for state.
			 */
			var i = models.length;
			while (i--) {
				if (typeof models[i].updateState === 'function') {
					models[i].updateState(this.HTTP);
				}
			}
			/*
			 * End hax
			 */
			this.clearTriggers(false);
			// Scans models for timeseries metrics and updates them.
			C.Util.updateTimeSeries(models, this.HTTP, this);

			// Scans models for aggregate metrics and udpates them.
			C.Util.updateAggregates(models, this.HTTP, this);

		},

		unload: function () {

			clearInterval(this.interval);

			this.set('elements', Em.Object.create());

		}

	});

	Controller.reopenClass({
		type: 'List',
		kind: 'Controller'
	});

	return Controller;

});
