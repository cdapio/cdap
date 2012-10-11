//
// Dashboard Controller
//

define([], function () {
	
	return Em.ArrayProxy.create({
		types: Em.Object.create(),
		load: function () {

			var self = this;

			self.set('types.Application', Em.ArrayProxy.create({content: []}));

			self.set('current', Em.Object.create({
				metricData: Em.Object.create()
			}));

			C.Ctl.List.getObjects('Application', function (objects) {
				var i = objects.length;
				while (i--) {
					objects[i] = C.Mdl['Application'].create(objects[i]);
				}
				self.get('types.Application').pushObjects(objects);
				self.getStats();

				C.interstitial.hide();
			});
		},
		testing: function () {
			console.log('trigg');
		}.property('current.metricData.processedcount'),
		__timeout: null,
		getStats: function () {

			var self = this, objects, content,
				end = Math.round(new Date().getTime() / 1000),
				start = end - C.__timeRange;

			C.get('monitor', {
				method: 'getTimeSeries',
				params: [null, null, ['processed.count', 'storage.trend'], start, end, 'ACCOUNT_LEVEL']
			}, function (error, response) {
				
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