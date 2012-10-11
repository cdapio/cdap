//
// Stream Model
//

define([], function () {
	return Em.Object.extend({
		type: 'Stream',
		href: function () {
			return '#/streams/' + this.get('id');
		}.property().cacheable(),
		init: function () {
			this._super();

			if (!this.get('id')) {
				this.set('id', this.get('name'));
			}

		},
		isSource: true,
		arrived: 0,
		storage: '0B',
		getUpdateRequest: function () {

			return [];

			/*

			var id = C.Ctl.Flow.current.id;
			var app = C.Ctl.Flow.current.app;

			var self = this;
			var pointCount = 30;

			var metrics = [];
			for (var name in this.get('metricNames')) {
				metrics.push(name);
			}

			var end = Math.round(new Date().getTime() / 1000),
				start = end - pointCount;

			return ['monitor', {
				method: 'getTimeSeries',
				params: [app, id, metrics, start, end, 'FLOWLET_LEVEL', this.get('id')]
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

					data = data.splice(0, 25);
					for (var k = data.length; k < 25; k++) {
						data.unshift(0);
					}

					metric = metric.replace(/\./g, '');

					self.get('metricData').set(metric, data);
					this.set('__loadingData', false);

				}

			}];

			*/

		}
	});
});