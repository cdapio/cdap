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

				self.set('types.Stream', Em.ArrayProxy.create());
				self.set('types.Flowlet', Em.ArrayProxy.create({content: objects}));
				
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

		updateStats: function () {
			var self = this;

			if (!this.get('current')) {
				self.startStats();
				return;
			}

			var app = this.get('current').applicationId;
			var id = this.get('current').get('meta').name;

			if (self.get('current').get('currentState') !== 'RUNNING') {
				self.startStats();
				return;
			}

			var end = Math.round(new Date().getTime() / 1000),
				start = end - 30;
			
			C.get.apply(C, this.get('current').getUpdateRequest());

			var names = [], flowletName, uri;

			var fs = C.Ctl.Flow.current.flowletStreams;
			switch (this.__currentFlowletLabel) {
				case 'arrival.count':
					for (flowletName in fs) {
						if (fs[flowletName].in) {
							uri = fs[flowletName].in.first;
							names.push('q.' + uri + '.enqueue.rate');
						}
					}
				break;
				case 'queue.depth':
					for (flowletName in fs) {
						if (fs[flowletName].in) {
							uri = fs[flowletName].in.first;
							names.push('q.' + uri + '.enqueue.count');
							names.push('q.' + uri + '.ack.' + flowletName + '.count');
						}
					}

			}

			console.log('going', names);

			C.get('monitor', {
				method: 'getCounters',
				params: [app, id, null, names]
			}, function (error, response) {
			
				console.log(arguments);

				if (C.router.currentState.name !== "flow") {
					return;
				}

				if (!response.params) {
					return;
				}

				var metrics = response.params;
				for (var i = 0; i < metrics.length; i ++) {

					if (self.__currentFlowletLabel === 'processed.count' &&
						metrics[i].name !== 'processed.count') {
						continue;
					}

					console.log(metrics[i].name);

					var finish = metrics[i].value;
					var flowlet = self.get_flowlet(metrics[i].qualifier);

					var fs = flowlet.streams;
					for (var j = 0; j < fs.length; j ++) {
						fs[j].set('metrics', finish);
					}

					finish = C.util.number(finish);
					flowlet.set('label', finish);

				}

				self.startStats();
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