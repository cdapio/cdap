/*
 * Stream Model
 */

define([], function () {

	var attachedFlow = Em.Object.extend({

		unconsumed: 0,

		init: function() {
			this._super();

			this.set('metricData', Em.Object.create());
			this.set('metricNames', {});

			this.set('id', this.get('flowId') || this.get('id') || this.get('meta').name);
			this.set('app', this.get('applicationId') || this.get('application'));

		},

		href: function () {
			return '#/flows/' + this.get('app') + ':' + this.get('id');
		}.property(),

		addMetricName: function (name) {

			this.get('metricNames')[name] = 1;

		},
		getUpdateRequest: function () {

			var metrics = [];
			var enqueueMetrics = [];

			for (var name in this.get('metricNames')) {

				if (name.indexOf('stream.enqueue') !== -1) {
					enqueueMetrics.push(name);
				} else {
					metrics.push(name);
				}

			}

			var accountId = C.Env.user.id;

			var start = C.__timeRange * -1;
			var self = this;

			var app = this.get('app');
			var id = this.get('id');

			C.get('monitor', {
				method: 'getTimeSeries',
				params: [app, id, enqueueMetrics, start, undefined, 'FLOW_LEVEL', this.get('id')]
			}, function (error, response, id) {

				if (!response.params) {
					return;
				}

				var data, points = response.params.points,
					latest = response.params.latest;

				for (var metric in points) {

					data = points[metric];

					var k = data.length;
					while(k --) {
						data[k] = data[k].value;
					}

					metric = metric.replace(/\./g, '');
					self.get('metricData').set(metric, data);

				}
			});

			var app = '-';
			var id = '-';

			var storageMetric = 'stream.storage.stream//' + accountId + '/' + this.get('id') + '.count';
			metrics.push(storageMetric);

			return ['monitor', {
				method: 'getTimeSeries',
				params: [app, id, metrics, start, undefined, 'FLOW_LEVEL', this.get('id')]
			}, function (error, response, id) {

				if (!response.params) {
					return;
				}

				var data, points = response.params.points,
					latest = response.params.latest;

				for (var metric in points) {

					data = points[metric];

					var k = data.length;
					while(k --) {
						data[k] = data[k].value;
					}

					metric = metric.replace(/\./g, '');
					self.get('metricData').set(metric, data);

				}


			}];

		}

	});

	var Model = Em.Object.extend({
		metricData: null,
		metricNames: null,
		type: 'Stream',
		plural: 'Streams',
		href: function () {
			return '#/streams/' + this.get('id');
		}.property().cacheable(),
		init: function() {
			this._super();

			this.set('metricData', Em.Object.create());
			this.set('metricNames', {});

			this.set('types', Em.Object.create());

			if (!this.get('id')) {
				this.set('id', this.get('name'));
			}

			var self = this;
			self.set('types.StreamFlow', Em.ArrayProxy.create({content: []}));

			C.get('metadata', {
				method: 'getFlowsByStream',
				params: [this.get('id')]
			}, function (error, response, id) {
				var flows = response.params;

				if (!flows) {
					return;
				}

				for (var i = 0; i < flows.length; i ++) {
					flows[i].streamId = id;
					self.get('types.StreamFlow').pushObject(attachedFlow.create(flows[i]));
				}

			}, this.get('id'));

		},
		isSource: true,
		arrived: 0,
		storage: 0,
		storageLabel: '0',
		storageUnit: 'B',
		unconsumed: 0,
		addMetricName: function (name) {

			this.get('metricNames')[name] = 1;

		},
		getUpdateRequest: function () {

			var metrics = [];
			for (var name in this.get('metricNames')) {
				metrics.push(name);
			}

			var flows = this.get('types.StreamFlow').content;
			for (var i = 0; i < flows.length; i ++) {
				C.get.apply(flows[i], flows[i].getUpdateRequest());
			}

			var app = '-';
			var id = '-';

			var accountId = C.Env.user.id;

			var start = C.__timeRange * -1;
			var self = this;

			var uri = 'stream//' + accountId + '/' + this.get('id');

			var storageMetric = 'stream.storage.' + uri + '.count';
			//metrics.push(storageMetric);

			// Used to track how many streamflows we are waiting for (to calc unconsumed)
			var streamFlows = this.get('types.StreamFlow').content;

			return ['monitor', {
				method: 'getTimeSeries',
				params: [app, id, metrics, start, undefined, 'ACCOUNT_LEVEL', this.get('id')]
			}, function (error, response, remain) {

				if (!response.params) {
					return;
				}

				var data, points = response.params.points,
					latest = response.params.latest;

				for (var metric in points) {

					data = points[metric];

					var k = data.length;
					while(k --) {
						data[k] = data[k].value;
					}

					metric = metric.replace(/\./g, '');
					self.get('metricData').set(metric, data);

				}

				C.get('monitor', {
					method: 'getCounters',
					params: ['-', '-', null, ['stream.enqueue.' + uri + '.count']]
				}, function (error, response) {

					if (!response.params || !response.params.length) {
						return;
					}

					var streamEnqueued = response.params[0].value;

					var flows = self.get('types.StreamFlow').content;
					for (var i = 0; i < flows.length; i ++) {

						var app = flows[i].application;
						var id = flows[i].id;
						var lowestAckd = Infinity;

						C.get('monitor', {
							method: 'getCounters',
							params: [app, id, null, ['q.ack.' + uri + '.count']]
						}, function (error, response, flow) {

							if (!response.params || !response.params.length) {
								return;
							}

							if (response.params[0].value < lowestAckd) {
								lowestAckd = response.params[0].value;
							}

							var diff = streamEnqueued - response.params[0].value;
							if (diff < 0) {
								diff = 0;
							}
							flow.set('unconsumed', C.Util.number(diff));

							if (--remain === 0) {
								diff = streamEnqueued - lowestAckd;
								if (diff < 0) {
									diff = 0;
								}
								self.set('unconsumed', C.Util.number(diff));
							}

						}, flows[i]);
					}
				});

				C.get('monitor', {
					method: 'getCounters',
					params: ['-', '-', null, [storageMetric]]
				}, function (error, response) {

					var storage;
					if (!response.params || !response.params.length) {
						storage = 0;
					} else {
						storage = response.params[0].value;
					}

					self.set('storage', storage);

					self.set('storageLabel', C.Util.bytes(storage)[0]);
					self.set('storageUnits', C.Util.bytes(storage)[1]);

				});

			}, streamFlows.length];

		}
	});

	Model.reopenClass({

		type: 'Stream',
		kind: 'Model',
		find: function (stream_id, http) {

			var promise = Ember.Deferred.create();

			C.get('metadata', {
				method: 'getStream',
				params: ['Stream', {
					id: stream_id
				}]
			}, function (error, response) {

				var model = C.Stream.create(response.params);

				promise.resolve(model);

			});

			return promise;
		}

	});

	return Model;

});