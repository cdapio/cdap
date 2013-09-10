/*
 * Flow Model
 */

define([], function () {

	var Model = Em.Object.extend({
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

			this.set('timeseries', Em.Object.create());
			this.set('name', (this.get('flowId') || this.get('id') || this.get('meta').name));

			this.set('app', this.get('applicationId') || this.get('app') || this.get('meta').app);
			this.set('id', this.get('app') + ':' +
				(this.get('flowId') || this.get('id') || this.get('meta').name));

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

		trackMetric: function (path, kind, label) {

			this.get(kind).set(path = this.interpolate(path), label || []);
			return path;

		},

		updateState: function (http) {

			var self = this;

			var app_id = this.get('app'),
				flow_id = this.get('name');

			http.rest('apps', app_id, 'flows', flow_id, 'status', function (response) {
				if (!jQuery.isEmptyObject(response)) {
					self.set('currentState', response.status);
				}
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
		}.property('meta'),

		isRunning: function() {

			if (this.currentState !== 'RUNNING') {
				return false;
			}
			return true;

		}.property('currentState'),
		started: function () {
			return this.lastStarted >= 0 ? $.timeago(this.lastStarted) : 'No Date';
		}.property('timeTrigger'),
		stopped: function () {
			return this.lastStopped >= 0 ? $.timeago(this.lastStopped) : 'No Date';
		}.property('timeTrigger'),
		actionIcon: function () {

			if (this.currentState === 'RUNNING' ||
				this.currentState === 'PAUSING') {
				return 'btn-pause';
			} else {
				return 'btn-start';
			}

		}.property('currentState').cacheable(false),
		stopDisabled: function () {

			if (this.currentState === 'RUNNING') {
				return false;
			}
			return true;

		}.property('currentState'),
		startDisabled: function () {
			if (this.currentState === 'RUNNING') {
				return true;
			}
			return false;
		}.property('currentState'),
		startPauseDisabled: function () {

			if (this.currentState !== 'STOPPED' &&
				this.currentState !== 'PAUSED' &&
				this.currentState !== 'DEPLOYED' &&
				this.currentState !== 'RUNNING') {
				return true;
			}
			return false;

		}.property('currentState'),
		defaultAction: function () {
			if (!this.currentState) {
				return '...';
			}
			return {
				'deployed': 'Start',
				'stopped': 'Start',
				'stopping': 'Start',
				'starting': 'Start',
				'running': 'Pause',
				'adjusting': '...',
				'draining': '...',
				'failed': 'Start'
			}[this.currentState.toLowerCase()];
		}.property('currentState')
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
					for (entry in model['flowletStreams']) {
						model['flowletStreams'][entry]['name'] = entry;
						flowletStreamArr.push(model['flowletStreams'][entry]);
					}
					model['flowletStreams'] = flowletStreamArr;
				}

				model.applicationId = app_id;
				model = C.Flow.create(model);
				
				http.rest('apps', app_id, 'flows', flow_id, 'status', function (response) {
					if (!jQuery.isEmptyObject(response)) {
						model.set('currentState', response.status);
						promise.resolve(model);
					} else {
						promise.reject('Status not found');
					}
				});

			});

			return promise;

		},

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
						instances: flowlet.instances,
					});

					if (!Em.isEmpty(flowlet.datasets)) {
						datasets.push.apply(datasets, flowlet.datasets);
					}

					var strObj = {};
					if (!jQuery.isEmptyObject(flowlet.inputs)) {
						if (flowlet.inputs.hasOwnProperty('')) {
							strObj['queue_IN'] = {
								second: 'IN'
							};
						}
					}
					if (!jQuery.isEmptyObject(flowlet.outputs)) {
						if (flowlet.outputs.hasOwnProperty('queue')) {
							strObj['queue_OUT'] = {
								second: 'OUT'
							};
						}
					}
					flowletStreams[descriptor] = strObj;
				}
			}
			obj.flowletStreams = flowletStreams;
			obj.datasets = datasets;
			obj.flowlets = flowlets;
			var connections = [];
			var flowStreams = [];
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
		}

	});

	return Model;

});
