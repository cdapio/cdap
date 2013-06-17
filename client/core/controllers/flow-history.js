/*
 * Flow History Controller
 */

define([], function () {

	var Controller = Ember.Controller.extend({

		runs: Ember.ArrayProxy.create({
			content: []
		}),

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
			this.set('elements.Batch', Em.ArrayProxy.create({content: objects}));

			C.get('manager', {
				method: 'getFlowHistory',
				params: [model.app, model.name]
			}, function (error, response) {

				var history = response.params;

				for (var i = 0; i < history.length; i ++) {

					self.runs.pushObject(C.Run.create(history[i]));

				}

			});

		},

		unload: function () {

			this.set('elements.Flowlet', Em.Object.create());
			this.set('elements.Stream', Em.Object.create());
			this.set('elements.Batch', Em.Object.create());

			this.get('runs').set('content', []);

		},

		loadRun: function (runId) {

			var app = this.get('model').app;
			var id = this.get('model').name;
			var self = this;

			C.get('monitor', {
				method: 'getCounters',
				params: [app, id, runId]
			}, function (error, response) {

				var metrics = response.params;
				var i = metrics.length;
				while (i--) {

					if (metrics[i].name === 'processed.count') {

						self.get_flowlet(metrics[i].qualifier).set('label', C.Util.number(metrics[i].value));

					}

				}

				$('#flowviz-container').removeClass('flowviz-fade');

			});

		},

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

		setFlowletLabel: function (event) {

			var label = $(event.target).html();
			this.get('controller').set('__currentFlowletLabel', {
				'Enqueue Rate': 'arrival.count',
				'Queue Size': 'queue.depth',
				'Max Queue Depth': 'queue.maxdepth',
				'Total Processed': 'processed.count'
			}[label]);

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
		type: 'FlowHistory',
		kind: 'Controller'
	});

	return Controller;

});