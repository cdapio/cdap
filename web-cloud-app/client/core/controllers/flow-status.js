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
				objects.push(C.Flowlet.create(flowlets[i]));
			}
			this.set('elements.Flowlet', Em.ArrayProxy.create({content: objects}));

			var streams = model.flowStreams;
			objects = [];
			for (var i = 0; i < streams.length; i ++) {
				objects.push(C.Stream.create(streams[i]));
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

		__currentFlowletLabel: 'processed.count',

		__setFlowletLabel: function (flowlet, value) {

			if (!flowlet) {
				return;
			}

			var fs = flowlet.streams;

			if (fs) {

				for (var j = 0; j < fs.length; j ++) {
					fs[j].set('metrics', value);
				}

			} else {
				flowlet.set('metrics', value);
			}

			value = C.Util.number(value);
			flowlet.set('label', value);

		},

		__resetFlowletLabels: function () {

			var content = this.elements.Flowlet.content;
			content.forEach(function (item) {

				item.set('label', '0');

			});

		},

		updateStats: function () {
			var self = this;

			// Update timeseries data for current flow.
			C.get.apply(C, this.get('model').getUpdateRequest());

			var app = this.get('model').applicationId;
			var id = this.get('model').get('meta').name;
			var end = Math.round(new Date().getTime() / 1000),
				start = end - 30;

			var names = [],
				flowletName,
				streamName,
				uri,
				fls = this.get('model').flowletStreams;

			var queueCalc = {}; // For use by 'queue'
			var queuedFor = {}; // For use by 'enqueued'

			var allMetrics = {};
			var accountId = C.Env.user.id;
			var metricType = 'processed';

			if (this.__currentFlowletLabel === 'arrival.count') {
				metricType = 'enqueued';
			} else if (this.__currentFlowletLabel === 'queue.depth') {
				metricType = 'queue';
			}

			var enqueue, ack;

			switch (metricType) {
				case 'processed':
					names = ['processed.count'];
					break;
				case 'enqueued':

					for (flowletName in fls) {
						queuedFor[flowletName] = [];
						for (streamName in fls[flowletName]) {
							// ITERATE OVER THE 'IN' STREAMS
							if (fls[flowletName][streamName].second === 'IN') {

								uri = fls[flowletName][streamName].first;
								uri = uri.replace(/:/g, '');
								uri = uri.replace(/_out/g, '');

								if (uri.indexOf('stream') === 0) {
									enqueue = 'stream.enqueue.' + uri + '.meanRate';
								} else {
									enqueue = 'q.enqueue.' + uri + '.meanRate';
								}
								names.push(enqueue);

								queuedFor[flowletName].push(enqueue);

							}
						}
					}

					break;
				case 'queue':

					for (flowletName in fls) {
						queueCalc[flowletName] = [];
						for (streamName in fls[flowletName]) {
							// ITERATE OVER THE 'IN' STREAMS
							if (fls[flowletName][streamName].second === 'IN') {

								uri = fls[flowletName][streamName].first;
								uri = uri.replace(/:/g, '');
								uri = uri.replace(/_out/g, '');

								if (uri.indexOf('stream') === 0) {
									enqueue = 'stream.enqueue.' + uri + '.count';
									ack = 'q.ack.' + uri + '.count';
								} else {
									enqueue = 'q.enqueue.' + uri + '.count';
									ack = 'q.ack.' + uri + '.count';
								}

								names.push(enqueue);
								names.push(ack);

								queueCalc[flowletName].push([enqueue, ack]);

							}
						}
					}

				break;
			}

			// Fill out metrics with defaults (server will not send defaults)
			for (var i = 0; i < names.length; i ++) {
				allMetrics[names[i]] = 0;
			}

			// This returns QUEUE metrics only. Stream metrics later.
			C.get('monitor', {
				method: 'getCounters',
				params: [app, id, null, names]
			}, function (error, response) {

				if (C.currentPath !== 'Flow.FlowStatus.index') {
					return;
				}

				if (!response.params) {
					return;
				}

				var metrics = response.params;
				for (var i = 0; i < metrics.length; i ++) {

					var metric = metrics[i].name,
						name = metrics[i].qualifier, flowlet;
					allMetrics[metric] = metrics[i].value;

					// Processed iterates through all metrics since they all have the same name.
					if (metricType === 'processed') {
						if (metric === 'processed.count') {
							flowlet = self.get_flowlet(name);
							self.__setFlowletLabel(flowlet, metrics[i].value);
						}
					}

				}

				var flowStreams = self.get('model').flowStreams;

				if (flowStreams.length || metricType === 'queue') {

					switch(metricType) {
						case 'processed':
							for (var i = 0 ; i < flowStreams.length; i ++) {
								names.push('stream.enqueue.stream//' + accountId + '/' + flowStreams[i].name + '.count');
							}
						break;
						case 'enqueue':
							for (var i = 0 ; i < flowStreams.length; i ++) {
								names.push('stream.enqueue.stream//' + accountId + '/' + flowStreams[i].name + '.meanRate');
							}
						break;
					}

					C.get('monitor', {
						method: 'getCounters',
						params: ['-', '-', null, names]
					}, function (error, response) {

						var metrics = response.params;
						for (var i = 0; i < metrics.length; i ++) {

							var metric = metrics[i].name;
							var name = metrics[i].qualifier;
							allMetrics[metric] = metrics[i].value;

						}

						var flowlet, uri, value, i;

						// Processed metric for Streams
						if (metricType === 'processed') {
							for (i = 0; i < flowStreams.length; i ++) {

								uri = 'stream.enqueue.stream//' + accountId + '/' + flowStreams[i].name + '.count';
								value = allMetrics[uri];

								flowlet = self.get_flowlet(flowStreams[i].name);
								self.__setFlowletLabel(flowlet, value || 0);

							}
						}

						// Enqueued rate metric for Streams
						if (metricType === 'enqueued') {
							for (i = 0; i < flowStreams.length; i ++) {

								uri = 'stream.enqueue.stream//' + accountId + '/' + flowStreams[i].name + '.meanRate';
								value = allMetrics[uri];

								flowlet = self.get_flowlet(flowStreams[i].name);
								self.__setFlowletLabel(flowlet, value);

							}

							// Enqueued looks up the value of a metric name (uri), given a flowlet.
							var flowlets = self.elements.Flowlet.content,
								i, uri, value;

							for (i = 0; i < flowlets.length; i ++) {
								uri = queuedFor[flowlets[i].id];
								value = allMetrics[uri];
								self.__setFlowletLabel(flowlets[i], value || 0);
							}

						}

						// Clear out label for queue size for Streams
						if (metricType === 'queue') {

							for (i = 0; i < flowStreams.length; i ++) {
								flowlet = self.get_flowlet(flowStreams[i].name);
								self.__setFlowletLabel(flowlet, 0);
							}

						}

						// Queued is calculated: subtract ack'd from enqueued
						for (var flowletName in queueCalc) {

							var total = 0;
							var streams = queueCalc[flowletName];

							for (var i = 0 ; i < streams.length; i ++) {
								var enqueued = allMetrics[streams[i][0]];
								var ackd = allMetrics[streams[i][1]];
								var diff = enqueued - ackd;
								if (diff > 0) {
									total += diff;
								}
							}

							var flowlet = self.get_flowlet(flowletName);
							self.__setFlowletLabel(flowlet, total);

						}

					});

				} else {

					// Enqueued looks up the value of a metric name (uri), given a flowlet.
					if (metricType === 'enqueued') {

						var flowlets = self.elements.Flowlet.content,
							i, uri, value;

						for (i = 0; i < flowlets.length; i ++) {
							uri = queuedFor[flowlets[i].id];
							value = allMetrics[uri];
							self.__setFlowletLabel(flowlets[i], value || 0);
						}
					}

				}

			});
		},

		/**
		 * Lifecycle
		 */

		start: function (app, id, version, config) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STARTING');

			C.get('manager', {
				method: 'start',
				params: [app, id, version, 'FLOW', config]
			}, function (error, response) {

				if (error) {
					C.Modal.show(error.name, error.message);
				} else {
					model.set('lastStarted', new Date().getTime() / 1000);
				}

			});

		},
		stop: function (app, id, version) {

			var self = this;
			var model = this.get('model');

			model.set('currentState', 'STOPPING');

			C.get('manager', {
				method: 'stop',
				params: [app, id, version]
			}, function (error, response) {

				if (error) {
					C.Modal.show(error.name, error.message);
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
		},
		"delete": function () {

			if (this.get('model').get('currentState') !== 'STOPPED' &&
				this.get('model').get('currentState') !== 'DEPLOYED') {
				C.Modal.show('Cannot Delete', 'Please stop the Flow before deleting.');
			} else {
				C.Modal.show(
					"Delete Flow",
					"You are about to remove a Flow, which is irreversible. You can upload this Flow again if you'd like. Do you want to proceed?",
					$.proxy(function (event) {

						var flow = this.get('model');

						C.get('far', {
							method: 'remove',
							params: [flow.app, flow.name, flow.version]
						}, function (error, response) {

							C.Modal.hide(function () {

								if (error) {
									C.Modal.show('Delete Error', error.message || 'No reason given. Please check the logs.');
								} else {
									window.history.go(-1);
								}

							});

						});
					}, this));
			}
		},

		setFlowletLabel: function (label) {

			this.set('__currentFlowletLabel', label);

			this.__resetFlowletLabels();

		},
		flowletLabelName: function () {

			return {
				'arrival.count': 'Enqueue Rate',
				'queue.depth': 'Queue Size',
				'queue.maxdepth': 'Max Queue Depth',
				'processed.count': 'Total Processed'
			}[this.__currentFlowletLabel];

		}.property('__currentFlowletLabel')

	});

	Controller.reopenClass({
		type: 'FlowStatus',
		kind: 'Controller'
	});

	return Controller;

});