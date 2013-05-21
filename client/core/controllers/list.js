//
// List Controller
//

define([], function () {

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),

		__plurals: {
			'App': 'Applications',
			'Flow': 'Process',
			'Stream': 'Collect',
			'Procedure': 'Query',
			'Dataset': 'Store'
		},
		title: function () {
			return this.__plurals[this.get('entityType')];
		}.property('entityType'),
		load: function (type) {

			var self = this;
			this.set('entityType', type);

			C.Api.getElements(type, function (objects) {

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
				}, 1000);

				/*
				 * Give the chart Embeddables 100ms to configure
				 * themselves before updating.
				 */
				setTimeout(function () {
					self.updateStats();
				}, 100);

			});

		},

		updateStats: function () {

			var objects, content, self = this;

			if ((objects = this.get('elements.' + this.get('entityType')))) {

				content = objects.get('content');

				for (var i = 0; i < content.length; i ++) {
					if (typeof content[i].getUpdateRequest === 'function') {
						C.get.apply(C, content[i].getUpdateRequest());
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