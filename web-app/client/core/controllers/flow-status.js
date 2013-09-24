/*
 * Flow Status Controller
 */

define([], function () {

	var Controller = Ember.Controller.extend({

		elements: Em.Object.create(),

		load: function () {

			this.clearTriggers(true);
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
				streams[i]['level'] = 'stream';
				objects.push(C.Stream.create(streams[i]));
				objects[i].trackMetric('/reactor/streams/{id}/collect.events', 'aggregates', 'events');

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

		ajaxCompleted: function () {
			return this.get('timeseriesCompleted') && this.get('aggregatesCompleted') &&
				this.get('ratesCompleted');
		},

		clearTriggers: function (value) {
			this.set('timeseriesCompleted', value);
			this.set('aggregatesCompleted', value);
			this.set('ratesCompleted', value);
		},

		updateStats: function () {
			if (!this.ajaxCompleted()) {
				return;
			}
			this.clearTriggers(false);
			this.get('model').updateState(this.HTTP);
			C.Util.updateTimeSeries([this.get('model')], this.HTTP, this);

			var models = this.get('elements.Flowlet.content').concat(
				this.get('elements.Stream.content'));

			C.Util.updateAggregates(models, this.HTTP, this);

			C.Util.updateRates(models, this.HTTP, this);

		},

		/**
		 * Lifecycle
		 */
		start: function (appId, id, config) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STARTING');
			this.HTTP.post('rest', 'apps', appId, 'flows', id, 'start', {
				data: config
			},
				function (response) {

					if (response.error) {
						C.Modal.show(response.error.name, response.error.message);
					} else {
						model.set('lastStarted', new Date().getTime() / 1000);
					}

			});

		},

		stop: function (appId, id) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STOPPING');

			this.HTTP.post('rest', 'apps', appId, 'flows', id, 'stop',
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

		setFlowletLabel: function (label) {

			var paths = {
				'rate': '/reactor/apps/{app}/flows/{flow}/flowlets/{id}/process.events.in',
				'pending': '/reactor/apps/{app}/flows/{flow}/flowlets/{id}/process.events.pending',
				'aggregate': '/reactor/apps/{app}/flows/{flow}/flowlets/{id}/process.events'
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
