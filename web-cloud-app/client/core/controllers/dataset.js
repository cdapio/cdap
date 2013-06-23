/*
 * Dataset Controller
 */

define([], function () {

	var attachedFlow = Em.Object.extend({

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
			for (var name in this.get('metricNames')) {
				metrics.push(name);
			}

			var app = '-';
			var id = '-';

			var accountId = C.Env.user.id;

			var app_id = this.get('app'),
				flow_id = this.get('id'),
				start = C.__timeRange * -1;
			var self = this;

			var storageMetric = 'dataset.storage.' + this.get('id') + '.count';
			metrics.push(storageMetric);

			C.get('manager', {
				method: 'status',
				params: [app_id, flow_id, -1]
			}, function (error, response) {

				if (response.params) {
					self.set('currentState', response.params.status);
				}

			});

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

	var Controller = Em.ArrayProxy.extend({

		types: Em.Object.create({
			'DatasetFlow': Em.ArrayProxy.create({
				content: []
			})
		}),
		interval: null,

		load: function () {

			var self = this;
			var model = this.get("model");

			C.get('metadata', {
				method: 'getFlowsByDataset',
				params: [model.id]
			}, function (error, response) {

				if (error) {
					return;
				}

				var flows = response.params;

				for (var i = 0; i < flows.length; i ++) {

					flows[i].datasetId = model.id;
					self.get('types.DatasetFlow').pushObject(attachedFlow.create(flows[i]));

				}

				self.interval = setInterval(function () {
					self.updateStats();
				}, C.POLLING_INTERVAL);
				/*
				 * Give the chart Embeddables 100ms to configure
				 * themselves before updating.
				 */
				setTimeout(function () {
					self.updateStats();
				}, C.EMBEDDABLE_DELAY);

			});

		},

		updateStats: function () {
			var self = this;

			// Update timeseries data for current flow.
			C.get.apply(C, this.get('model').getUpdateRequest());

			var flows = this.get('types.DatasetFlow').content;
			for (var i = 0; i < flows.length; i ++) {
				C.get.apply(flows[i], flows[i].getUpdateRequest());
			}

		},

		unload: function () {

			this.set('types', Em.Object.create({
				'DatasetFlow': Em.ArrayProxy.create({
					content: []
				})
			}));

			clearInterval(this.interval);

		},

		"delete": function () {

			C.Modal.show(
				"Delete Dataset",
				"Are you sure you would like to delete this Dataset? This action is not reversible.",
				$.proxy(function (event) {

					var model = this.get('model');

					C.get('metadata', {
						method: 'deleteDataset',
						params: ['Dataset', {
							id: model.id
						}]
					}, function (error, response) {

						if (error) {
							C.Modal.show('Delete Error', error.message);
						} else {
							window.history.go(-1);
						}

					});
				}, this));

		}

	});

	Controller.reopenClass({
		type: 'Dataset',
		kind: 'Controller'
	});

	return Controller;

});