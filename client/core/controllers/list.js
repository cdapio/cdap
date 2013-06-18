/*
 * List Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),

		__plurals: {
			'App': 'Applications',
			'Flow': 'Process',
			'Batch': 'Process',
			'Stream': 'Collect',
			'Procedure': 'Query',
			'Dataset': 'Store'
		},
		entityTypes: new Em.Set(),
		title: function () {
			return this.__plurals[this.get('entityType')];
		}.property('entityType'),
		load: function (type) {

			var self = this;
			this.set('entityType', type);

			this.entityTypes.add(type);

			this.HTTP.getElements(type, function (objects) {

				//** Hax: Remove special case for Flow when ready **//

				var i = objects.length;
				while (i--) {
					if (type === 'Query' && objects[i].type === 0) {
						objects.splice(i, 1);
					} else if (type === 'Flow' && objects[i].type === 1) {
						objects.splice(i, 1);
					}
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
							C.get.apply(C, content[i].getUpdateRequest());
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