/*
 * Flow Status Controller
 */

define([], function () {

	var Controller = Ember.Controller.extend({

		elements: Em.Object.create(),

		load: function () {

			var model = this.get('model');
			var self = this;

			var flowlets = model.flowlets;
			var objects = [];
			for (var i = 0; i < flowlets.length; i ++) {

				flowlets[i].flow = model.get('name');
				flowlets[i].app = model.get('app');

				objects.push(C.Flowlet.create(flowlets[i]));

			}
			this.set('elements.Flowlet', Em.ArrayProxy.create({content: objects}));

			this.setFlowletLabel('aggregate');

			var streams = model.flowStreams;
			objects = [];
			for (var i = 0; i < streams.length; i ++) {

				objects.push(C.Stream.create(streams[i]));
				objects[i].trackMetric('/collect/events/streams/{id}', 'aggregates', 'events');

			}
			this.set('elements.Stream', Em.ArrayProxy.create({content: objects}));

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

			this.set('elements.Flowlet', Em.Object.create());
			this.set('elements.Stream', Em.Object.create());

			clearInterval(this.interval);

		},

		statusButtonAction: function () {
			return 'No Action';
		}.property(),
		statusButtonClass: function () {
			return 'btn btn-warning';
		}.property(),

		get_flowlet: function (id) {
			id = id + "";
			var content = this.elements.Flowlet.content;
			for (var k = 0; k < content.length; k++) {
				if (content[k].name === id) {
					return content[k];
				}
			}
			content = this.elements.Stream.content;
			for (k = 0; k < content.length; k++) {
				if (content[k].name === id) {
					return content[k];
				}
			}
		},

		updateStats: function () {

			this.get('model').updateState(this.HTTP);
			C.Util.updateTimeSeries([this.get('model')], this.HTTP);

			var models = this.get('elements.Flowlet.content').concat(
				this.get('elements.Stream.content'));

			C.Util.updateAggregates(models, this.HTTP);

			C.Util.updateRates(models, this.HTTP);

		},

		/**
		 * Lifecycle
		 */
		start: function (app, id, version, config) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STARTING');

			this.HTTP.rpc('runnable', 'start', [app, id, version, 'FLOW', config],
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

			this.HTTP.rpc('runnable', 'stop', [app, id, version, 'FLOW'],
				function (response) {

					if (response.error) {
						C.Modal.show(response.error.name, response.error.message);
					}

			});

		},

		/**
		 * Action handlers from the View
		 */
		config: function () {

			var self = this;
			var model = this.get('model');

			this.transitionToRoute('FlowStatus.Config');

		},

		exec: function () {
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

			var self = this;

			if (this.get('model').get('currentState') !== 'STOPPED' &&
				this.get('model').get('currentState') !== 'DEPLOYED') {
				C.Modal.show('Cannot Delete', 'Please stop the Flow before deleting.');
			} else {
				C.Modal.show(
					"Delete Flow",
					"You are about to remove a Flow, which is irreversible. You can upload this Flow again if you'd like. Do you want to proceed?",
					$.proxy(function (event) {

						var flow = this.get('model');

						self.HTTP.rpc('runnable', 'remove', [flow.app, flow.name, flow.version],
							function (response) {

							C.Modal.hide(function () {

								if (response.error) {
									C.Modal.show('Delete Error', response.error.message || 'No reason given. Please check the logs.');
								} else {
									window.history.go(-1);
								}

							});

						});
					}, this));
			}
		},

		setFlowletLabel: function (label) {

			var paths = {
				'rate': '/process/events/{app}/flows/{flow}/{id}/ins',
				'pending': '/process/events/{app}/flows/{flow}/{id}/pending',
				'aggregate': '/process/events/{app}/flows/{flow}/{id}'
			};
			var kinds = {
				'rate': 'rates',
				'pending': 'aggregates',
				'aggregate': 'aggregates'
			};

			var flowlets = this.get('elements.Flowlet.content');
			var streams = this.get('elements.Stream.content');

			var i = flowlets.length;
			while (i--) {
				flowlets[i].clearMetrics();
				flowlets[i].trackMetric(paths[label], kinds[label], 'events');
			}

			this.set('__currentFlowletLabel', label);

		},
		flowletLabelName: function () {

			return {
				'rate': 'Flowlet Rate',
				'pending': 'Flowlet Pending',
				'aggregate': 'Flowlet Processed'
			}[this.__currentFlowletLabel];

		}.property('__currentFlowletLabel')

	});

	Controller.reopenClass({
		type: 'FlowStatus',
		kind: 'Controller'
	});

	return Controller;

});
