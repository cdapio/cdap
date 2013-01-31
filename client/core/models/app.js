//
// App Model
//

define([], function () {
	return Em.Object.extend({
		href: function () {
			return '#/apps/' + this.get('id');
		}.property(),
		storage: '0B',
		metricData: null,
		metricNames: null,
		__loadingData: false,
		init: function() {
			this._super();

			this.set('metricData', Em.Object.create());
			this.set('metricNames', {});

			this.set('counts', {
				Stream: 0,
				Flow: 0,
				Dataset: 0,
				Query: 0
			});

		},
		addMetricName: function (name) {

			this.get('metricNames')[name] = 1;

		},
		getUpdateRequest: function (done) {

			var self = this;

			var id = this.get('id'),
				start = C.__timeRange * -1;

			var metrics = [];
			var metricNames = this.get('metricNames');

			for (var name in metricNames) {
				if (metricNames[name] === 1) {
					metrics.push(name);
				}
			}
			if (!metrics.length) {
				C.debug('Cannot update. Not tracking any metrics for APP: ' + id);
				this.set('__loadingData', false);
				return;
			}
				
			return ['monitor', {
				method: 'getTimeSeries',
				params: [id, null, metrics, start, undefined, 'APPLICATION_LEVEL']
			}, function (error, response, params) {

				if (self.get('isDestroyed')) {
					return;
				}
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

					/*
					data = data.splice(0, 25);
					for (var k = data.length; k < 25; k++) {
						data.unshift(0);
					}
					*/

					metric = metric.replace(/\./g, '');
					self.get('metricData').set(metric, data);

				}

				if (typeof done === 'function') {
					done();
				}

			}];
		}
	});
});