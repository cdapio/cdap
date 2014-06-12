/*
 * Flow Model
 */

define(['core/models/program'], function (Program) {

	var Model = Program.extend({
		type: 'Flow',
		plural: 'Flows',
		href: function () {
			return '#/flows/' + this.get('id');
		}.property('id'),

		instances: 0,
		version: -1,
		currentState: '',

		init: function() {

			this._super();

			this.set('name', (this.get('flowId') || this.get('id') || this.get('meta').name));

			this.set('app', this.get('applicationId') || this.get('app') || this.get('meta').app);
			this.set('id', this.get('app') + ':' +
				(this.get('flowId') || this.get('id') || this.get('meta').name));

			this.set('description', this.get('meta') || 'Flow');

		},

		/*
		 * Runnable context path, used by user-defined metrics.
		 */
		context: function () {

			return this.interpolate('/apps/{parent}/flows/{id}');

		}.property('app', 'name'),

		interpolate: function (path) {

			return path.replace(/\{parent\}/, this.get('app'))
				.replace(/\{id\}/, this.get('name'));

		},

		getSubPrograms: function (callback, http) {

			var app = this.get('app');
			var flow = this.get('name');

			http.rest('apps', this.get('app'), 'flows', this.get('name'), function (model) {

				var flowlets = [];

				for (var name in model.flowlets) {
					flowlets.push(C.Flowlet.create({
						type: 'Flowlet',
						app: app,
						flow: flow,
						name: name
					}));
				}

				callback({ 'Flowlet': flowlets });

			});

		},

		getMeta: function () {
			var arr = [];
			for (var m in this.meta) {
				arr.push({
					k: m,
					v: this.meta[m]
				});
			}
			return arr;
		}.property('meta')

	});

	Model.reopenClass({
		type: 'Flow',
		kind: 'Model',
		find: function(model_id, http) {
			var self = this;
			var promise = Ember.Deferred.create();

			var model_id = model_id.split(':');
			var app_id = model_id[0];
			var flow_id = model_id[1];

			http.rest('apps', app_id, 'flows', flow_id, function (model, error) {

				var model = self.transformModel(model);
				if (model.hasOwnProperty('flowletStreams')) {
					var flowletStreamArr = [];
					for (var entry in model['flowletStreams']) {
						model['flowletStreams'][entry]['name'] = entry;
						flowletStreamArr.push(model['flowletStreams'][entry]);
					}
					model['flowletStreams'] = flowletStreamArr;
				}

				model.applicationId = app_id;
				model = C.Flow.create(model);

				http.rest('apps', app_id, 'flows', flow_id, 'status', function (response) {
					if (!$.isEmptyObject(response)) {
						model.set('currentState', response.status);
						promise.resolve(model);
					} else {
						promise.reject('Status not found');
					}
				});

			});

			return promise;

		},

		/**
		 * HAX. Transforms v2 json to v1 format for rendering flow status diagram.
		 * TODO: Change frontend to accept v2 json and remove this code.
		 */
		transformModel: function (model) {
			var obj = {};
			var meta = {};
			if (model.hasOwnProperty('name')) {
				meta.name = model.name;
			}
			obj.meta = meta;
			var datasets = [];
			var flowlets = [];
			var flowletStreams = {};
			if (model.hasOwnProperty('flowlets')) {
				for (var descriptor in model.flowlets) {
					var flowlet = model.flowlets[descriptor];
					flowlets.push({
						name: flowlet.flowletSpec.name,
						classname: flowlet.flowletSpec.className,
						instances: flowlet.instances
					});

					if (!Em.isEmpty(flowlet.datasets)) {
						datasets.push.apply(datasets, flowlet.datasets);
					}

					var strObj = {};
					if (!$.isEmptyObject(flowlet.inputs)) {
						strObj['queue_IN'] = {
							second: 'IN'
						};
					}
					if (!$.isEmptyObject(flowlet.outputs)) {
						strObj['queue_OUT'] = {
							second: 'OUT'
						};
					}
					flowletStreams[descriptor] = strObj;
				}
			}
			obj.flowletStreams = flowletStreams;
			obj.datasets = datasets;
			obj.flowlets = flowlets;
			var connections = [];
			var flowStreams = [];
			model.connections = this.validateConnections(model.connections);
			for (var i = 0; i < model.connections.length; i++) {
				var cn = model.connections[i];
				var from = {};
				var to = {};
				from[cn.sourceType.toLowerCase()] = cn.sourceName;
				to['flowlet'] = cn.targetName;
				connections.push({
					from: from,
					to: to
				});
				if (cn.sourceType === 'STREAM') {
					flowStreams.push({
						name: cn.sourceName
					});
				}
			}
			obj.flowStreams = flowStreams;

			obj.connections = connections;
			return obj;
		},

		/**
		 * Validates connections and inserts a dummy node where there are overlapping flowlets.
		 * @param  {Array} connections JSON received from server.
		 * @return {Array} Validated connections with dummy nodes appropriately inserted.
		 */
		validateConnections: function (connections) {
			var assignments = {};

			// First determine which order the nodes are rendered visually. This is based on a horizontal
			// column format.
			for (var i = 0, len = connections.length; i < len; i++) {
				var conn = connections[i];
				if (!(conn['sourceName'] in assignments)) {
					assignments[conn['sourceName']] = 0;
				}
				if (!(conn['targetName'] in assignments)) {
					assignments[conn['targetName']] = assignments[conn['sourceName']] + 1;
				}
			}

			// Determine if there are any anomolies i.e. nodelevel3 --> nodelevel3 and increment to
			// nodelevel3 --> nodelevel4.
			for (var i = 0, len = connections.length; i < len; i++) {
				var conn = connections[i];
				if (assignments[conn['sourceName']] === assignments[conn['targetName']]) {
					assignments[conn['targetName']]++;
				}
			}

			// Set up dummy connections if anomoly is detected and there is distance between connecting
			// nodes. This changes connection nodelevel2 --> nodelevel5 to:
			// nodelevel2 --> dummylevel3, dummylevel3 --> dummylevel4, dummylevel4 --> nodelevel5.
			var newConnections = [];
			for (var i = 0, len = connections.length; i < len; i++) {
				var source = connections[i].sourceName;
				var destination = connections[i].targetName;
				if (assignments[destination] - assignments[source] > 1) {
					var diff = assignments[destination] - assignments[source];
					for (var z = 0; z < diff; z++) {
						if (z === 0) {
							newConnections.push({
								sourceType: connections[i].sourceType,
								sourceName: connections[i].sourceName,
								targetName: 'dummy'
							});
						} else if (z > 0 && z !== diff -1) {
							newConnections.push({
								sourceType: 'FLOWLET',
								sourceName: 'dummy',
								targetName: 'dummy'
							});
						} else if (z === diff - 1) {
							newConnections.push({
								sourceType: 'FLOWLET',
								sourceName: 'dummy',
								targetName: connections[i].targetName
							});
						}
					}
				} else {
					newConnections.push(connections[i]);
				}
			}
			return newConnections;
		}

	});

	return Model;

});
