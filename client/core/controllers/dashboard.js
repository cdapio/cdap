//
// Dashboard Controller
//

define([], function () {
	
	return Em.ArrayProxy.create({
		types: Em.Object.create(),
		counts: Em.Object.create(),
		load: function () {

			var self = this;

			this.set('types.Application', Em.ArrayProxy.create({content: []}));
			this.set('current', Em.Object.create({
				addMetricName: function (metric) {
					this.get('metricNames')[metric] = 1;
				},
				metricNames: {},
				metricData: Em.Object.create()
			}));

			C.Ctl.List.getObjects('Application', function (objects) {
				var i = objects.length;
				while (i--) {
					objects[i] = C.Mdl['Application'].create(objects[i]);

					C.Ctl.List.getObjects('Stream', function (obj) {
						objects[i + 1].set('counts.Stream', obj.length);
					});
					C.Ctl.List.getObjects('Flow', function (obj) {
						objects[i + 1].set('counts.Flow', obj.length);
					});
					C.Ctl.List.getObjects('Dataset', function (obj) {
						objects[i + 1].set('counts.Dataset', obj.length);
					});
					C.Ctl.List.getObjects('Query', function (obj) {
						objects[i + 1].set('counts.Query', obj.length);
					});

				}
				self.get('types.Application').pushObjects(objects);
				self.getStats();

				self.get('counts').set('Application', objects.length);

				C.interstitial.hide();

			});

			C.Ctl.List.getObjects('Stream', function (objects) {
				self.get('counts').set('Stream', objects.length);
			});
			C.Ctl.List.getObjects('Flow', function (objects) {
				self.get('counts').set('Flow', objects.length);
			});
			C.Ctl.List.getObjects('Dataset', function (objects) {
				self.get('counts').set('Dataset', objects.length);
			});
			C.Ctl.List.getObjects('Query', function (objects) {
				self.get('counts').set('Query', objects.length);
			});

		},
		testing: function () {
			console.log('trigg');
		}.property('current.metricData.processedcount'),
		__timeout: null,
		getStats: function () {

			var self = this, objects, content;

			C.get('monitor', {
				method: 'getTimeSeries',
				params: [null, null, ['processed.count', 'storage.trend'], (C.__timeRange * -1), null, 'ACCOUNT_LEVEL']
			}, function (error, response) {
				
				if (!response.params) {
					return;
				}

				var data, points = response.params.points;

				for (var metric in points) {
					data = points[metric];

					var k = data.length;
					while(k --) {
						data[k] = data[k].value;
					}

					metric = metric.replace(/\./g, '');
					self.get('current').get('metricData').set(metric, data);

				}


			});

			if ((objects = this.get('types.Application'))) {

				content = objects.get('content');

				for (var i = 0; i < content.length; i ++) {
					if (typeof content[i].getUpdateRequest === 'function') {
						C.get.apply(C, content[i].getUpdateRequest());
					}
				}

				self.__timeout = setTimeout(function () {
					self.getStats();
				}, 1000);
			}

		},

		unload: function () {
			clearTimeout(this.__timeout);
			this.set('types', Em.Object.create());
		}
	});
});