//
// Flow Controller.
//

define([], function () {

	return Em.Object.create({
		current: null,
		types: Em.Object.create(),
		statusButtonAction: function () {
			return 'No Action';
		}.property(),
		statusButtonClass: function () {
			return 'btn btn-warning';
		}.property(),

		get_flowlet: function (id) {
			id = id + "";
			var content = this.types.Flowlet.content;
			for (var k = 0; k < content.length; k++) {
				if (content[k].name === id) {
					return content[k];
				}
			}
			content = this.types.Stream.content;
			for (k = 0; k < content.length; k++) {
				if (content[k].name === id) {
					return content[k];
				}
			}
		},

		unload: function () {

			clearTimeout(this.updateTimeout);
			this.set('types.Flowlet', Em.Object.create());
			this.set('types.Stream', Em.Object.create());
			this.set('current', null);
			clearInterval(this.interval);

		},

		load: function (app, id) {

			var self = this;

			C.interstitial.loading();
			C.get('manager', {
				method: 'getFlowDefinition',
				params: [app, id]
			}, function (error, response) {

				if (error) {
					C.interstitial.label(error);
					return;
				}

				if (!response.params) {
					C.interstitial.label('Flow not found.', {
						action: 'All Flows',
						click: function () {
							C.router.set('location', '');
						}
					});
					return;
				}

				response.params.currentState = 'UNKNOWN';
				response.params.version = -1;
				response.params.type = 'Flow';
				response.params.applicationId = app;

				self.set('current', C.Mdl.Flow.create(response.params));
	
				var flowlets = response.params.flowlets;
				var objects = [];
				for (var i = 0; i < flowlets.length; i ++) {
					objects.push(C.Mdl.Flowlet.create(flowlets[i]));
				}
				self.set('types.Flowlet', Em.ArrayProxy.create({content: objects}));
				
				var streams = response.params.flowStreams;
				objects = [];
				for (var i = 0; i < streams.length; i ++) {
					objects.push(C.Mdl.Stream.create(streams[i]));
				}
				self.set('types.Stream', Em.ArrayProxy.create({content: objects}));

				C.router.applicationController.view.visualizer.drawGraph();



				//
				// Request Flow Status
				//
				C.get('manager', {
					method: 'status',
					params: [app, id, -1]
				}, function (error, response) {

					if (response.params) {

						self.get('current').set('currentState', response.params.status);
						self.get('current').set('version', response.params.version);

						self.updateStats();

						C.interstitial.hide();
					
					} else {
						C.interstitial.label('Unable to get Flow Status.');
					}

					self.interval = setInterval(function () {
						self.refresh();
					}, 1000);

				});

			});

		},
		refresh: function () {

			var self = this;
			var app = this.get('current').applicationId;
			var id = this.get('current').get('meta').name;

			if (this.__pending) {
				return;
			}

			C.get('manager', {
				method: 'status',
				params: [app, id, -1]
			}, function (error, response) {

				if (response.params && self.get('current')) {
					self.get('current').set('currentState', response.params.status);
				}
			});
		},
		
		startStats: function () {
			var self = this;
			clearTimeout(this.updateTimeout);
			this.updateTimeout = setTimeout(function () {
				self.updateStats();
			}, 1000);
		},

		__currentFlowletLabel: 'processed.count',

		__setFlowletLabel: function (flowlet, value) {

			if (!flowlet) {
				console.log('No flowlet to set label', value);
			}

			var fs = flowlet.streams;

			if (fs) {

				for (var j = 0; j < fs.length; j ++) {
					fs[j].set('metrics', value);
				}

			} else {
				flowlet.set('metrics', value);
			}

			value = C.util.number(value);
			flowlet.set('label', value);

		},

		__resetFlowletLabels: function () {

			var content = this.types.Flowlet.content;
			content.forEach(function (item) {

				item.set('label', '0');

			});

		},

		updateStats: function () {
			var self = this;

			if (!this.get('current')) {
				self.startStats();
				return;
			}

			// Update timeseries data for current flow.
			C.get.apply(C, this.get('current').getUpdateRequest());

			var app = this.get('current').applicationId;
			var id = this.get('current').get('meta').name;
			var end = Math.round(new Date().getTime() / 1000),
				start = end - 30;

			var names = [],
				flowletName,
				streamName,
				uri,
				fls = C.Ctl.Flow.current.flowletStreams;

			var queueCalc = {}; // For use by 'queue'
			var queuedFor = {}; // For use by 'enqueued'

			var allMetrics = {};
			var accountId = 'demo';
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
			
				if (C.router.currentState.name !== "flow") {
					return;
				}

				if (!response.params) {
					return;
				}

				//if (metricType === 'processed' || metricType === 'enqueued') {
					//self.__resetFlowletLabels(0);
				//}

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

				var flowStreams = C.Ctl.Flow.current.flowStreams;

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
							var flowlets = C.Ctl.Flow.types.Flowlet.content,
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
								total += (enqueued - ackd);
							}

							var flowlet = self.get_flowlet(flowletName);
							self.__setFlowletLabel(flowlet, total);

						}

						self.startStats();

					});

				} else {

					// Enqueued looks up the value of a metric name (uri), given a flowlet.
					if (metricType === 'enqueued') {

						var flowlets = C.Ctl.Flow.types.Flowlet.content,
							i, uri, value;

						for (i = 0; i < flowlets.length; i ++) {
							uri = queuedFor[flowlets[i].id];
							value = allMetrics[uri];
							self.__setFlowletLabel(flowlets[i], value || 0);
						}
					}

					self.startStats();

				}

			});
		},
		/*
		intervals: {},
		stop_spin: function () {
			for (var i in this.intervals) {
				clearInterval(this.intervals[i]);
				delete this.intervals[i];
			}
		},
		spinner: function (element, start, finish, incr) {
			var id = element.attr('id');
			var res = 10;
			incr *= res;

			var interval = this.intervals[id] = setInterval(function () {
				element.html(Math.ceil(start));
				start += incr;
				if (start >= finish) {
					clearInterval(interval);
					element.html(finish);
				}
			}, res);
		},
		spins: function (element, start, finish, time) {
			if (start === finish) {
				element.html(finish);
				return;
			}
			var incr = (finish - start) / time;
			this.spinner(element, start, finish, incr);
		},
		*/

		start: function (app, id, version) {

			$('#flow-alert').hide();

			var thisFlow = C.Ctl.Flow.current;
			var self = this;

			self.__pending = true;
			thisFlow.set('currentState', 'STARTING');

			C.socket.request('manager', {
				method: 'start',
				params: [app, id, version]
			}, function (error, response) {

				self.__pending = false;

				thisFlow.set('lastStarted', new Date().getTime() / 1000);

				if (C.Ctl.Flow.current) {
					C.Ctl.Flow.updateStats();
				}

			});

		},
		stop: function (app, id, version) {

			$('#flow-alert').hide();

			var thisFlow = C.Ctl.Flow.current;
			var self = this;
			
			self.__pending = true;
			thisFlow.set('currentState', 'STOPPING');

			C.socket.request('manager', {
				method: 'stop',
				params: [app, id, version]
			}, function (error, response) {

				self.__pending = false;

			});

		}
	});
});