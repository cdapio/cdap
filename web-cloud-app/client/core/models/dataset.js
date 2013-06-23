/*
 * Dataset Model
 */

define([], function () {

	var Model = Em.Object.extend({

		href: function () {
			return '#/datasets/' + this.get('id');
		}.property(),
		metricData: null,
		metricNames: null,
		type: 'Dataset',
		plural: 'Datasets',
		init: function() {
			this._super();

			this.set('metricData', Em.Object.create());
			this.set('metricNames', {});

			if (!this.get('id')) {
				this.set('id', this.get('name'));
			}

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

			var app = '-';
			var id = '-';

			var accountId = C.Env.user.id;

			var start = C.__timeRange * -1;
			var self = this;

			var storageMetric = 'dataset.storage.' + this.get('id') + '.count';
			//metrics.push(storageMetric);

			return ['monitor', {
				method: 'getTimeSeries',
				params: [app, id, metrics, start, undefined, 'ACCOUNT_LEVEL', this.get('id')]
			}, function (error, response) {

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

			}];

		}
	});

	Model.reopenClass({
		type: 'Dataset',
		kind: 'Model',
		find: function (dataset_id, http) {
			var promise = Ember.Deferred.create();

			C.get('metadata', {
				method: 'getDataset',
				params: ['Dataset', {
					id: dataset_id
				}]
			}, function (error, response) {

				var model = C.Dataset.create(response.params);

				promise.resolve(model);

			});

			return promise;
		}
	});

	return Model;

});