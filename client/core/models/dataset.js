//
// Dataset Model
//

define([], function () {
	return Em.Object.extend({
		metricData: null,
		metricNames: null,
		type: 'Dataset',
		plural: 'Datasets',
		href: function () {
			return '#/data/' + this.get('id');
		}.property().cacheable(),
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

			var accountId = 'demo';

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

					self.set('storageLabel', C.util.bytes(storage)[0]);
					self.set('storageUnits', C.util.bytes(storage)[1]);

				});

			}];

		}
	});
});