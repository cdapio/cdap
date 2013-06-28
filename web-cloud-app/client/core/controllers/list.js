/*
 * List Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),

		__titles: {
			'App': 'Applications',
			'Flow': 'Process',
			'Batch': 'Process',
			'Stream': 'Collect',
			'Procedure': 'Query',
			'Dataset': 'Store'
		},
		__plurals: {
			'App': 'apps',
			'Flow': 'flows',
			'Batch': 'mapreduce',
			'Stream': 'streams',
			'Procedure': 'procedures',
			'Dataset': 'datasets'
		},
		entityTypes: new Em.Set(),
		title: function () {
			return this.__titles[this.get('entityType')];
		}.property('entityType'),
		load: function (type) {

			var self = this;
			this.set('entityType', type);

			this.entityTypes.add(type);

			this.HTTP.get('rest', this.__plurals[type], function (objects) {

				var i = objects.length;
				while (i--) {
					objects[i] = C[type].create(objects[i]);
				}

				self.set('elements.' + type, Em.ArrayProxy.create({content: objects}));

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

		updateStats: function () {

			var content, self = this;
			for (var j=0; j<this.entityTypes.length; j++) {
				var objects = this.get('elements.' + this.entityTypes[j]);

				if (objects) {

					content = objects.get('content');

					for (var i = 0; i < content.length; i ++) {
						if (typeof content[i].getUpdateRequest === 'function') {
							C.get.apply(C, content[i].getUpdateRequest(this.HTTP));
						}
					}

				}
			}

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