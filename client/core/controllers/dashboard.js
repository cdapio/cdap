//
// Dashboard Controller
//

define([], function () {
	
	return Em.ArrayProxy.create({
		types: Em.Object.create(),
		counts: Em.Object.create(),
		__toLoad: 5,
		load: function () {

			var self = this;

			this.__toLoad = 5;

			this.set('types.Application', Em.ArrayProxy.create({content: []}));
			this.set('types.Stream', Em.ArrayProxy.create({content: []}));
			this.set('types.Dataset', Em.ArrayProxy.create({content: []}));

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

					C.Ctl.List.getObjects('Stream', function (obj, arg) {
						objects[arg].set('counts.Stream', obj.length);
					}, objects[i].id, i);
					C.Ctl.List.getObjects('Flow', function (obj, arg) {
						objects[arg].set('counts.Flow', obj.length);
					}, objects[i].id, i);
					C.Ctl.List.getObjects('Dataset', function (obj, arg) {
						objects[arg].set('counts.Dataset', obj.length);
					}, objects[i].id, i);
					C.Ctl.List.getObjects('Query', function (obj, arg) {
						objects[arg].set('counts.Query', obj.length);
					}, objects[i].id, i);

				}
				self.get('types.Application').pushObjects(objects);
				self.get('counts').set('Application', objects.length);

				self.__toLoad--;
				if (!self.__toLoad) {
					C.interstitial.hide();
					self.getStats();
				}

			});

			C.Ctl.List.getObjects('Stream', function (objects) {

				// For use by storage trend
				self.get('types.Stream').pushObjects(objects);
				
				// For use by account stream count
				self.get('counts').set('Stream', objects.length);

				self.__toLoad--;
				if (!self.__toLoad) {
					C.interstitial.hide();
					self.getStats();
				}

			});
			C.Ctl.List.getObjects('Flow', function (objects) {
				self.get('counts').set('Flow', objects.length);
				
				self.__toLoad--;
				if (!self.__toLoad) {
					C.interstitial.hide();
					self.getStats();
				}
			});
			C.Ctl.List.getObjects('Dataset', function (objects) {

				// For use by storage trend
				self.get('types.Dataset').pushObjects(objects);

				// For use by account datset count
				self.get('counts').set('Dataset', objects.length);

				self.__toLoad--;
				if (!self.__toLoad) {
					C.interstitial.hide();
					self.getStats();
				}
			});
			C.Ctl.List.getObjects('Query', function (objects) {
				self.get('counts').set('Query', objects.length);

				self.__toLoad--;
				if (!self.__toLoad) {
					C.interstitial.hide();
					self.getStats();
				}
			});

		},
		__timeout: null,
		getStats: function () {

			var self = this, objects, content;

			var streams = this.get('types.Stream').content;
			var datasets = this.get('types.Dataset').content;

			var accountId = window.ENV.account.account_id;
			var metrics = [];
			var start = C.__timeRange * -1;

			for (var i = 0; i < streams.length; i ++) {
				metrics.push('stream.storage.stream//' + accountId + '/' + streams[i].id + '.count');
			}

			for (var i = 0; i < datasets.length; i ++) {
				metrics.push('dataset.storage.' + datasets[i].id + '.count');
			}

			if (metrics.length) {

				C.get('monitor', {
					method: 'getCounters',
					params: ['-', '-', null, metrics]
				}, function (error, response) {

					if (!response.params) {
						return;
					}

					var values = response.params;
					var currentValue = 0;
					for (var i = 0; i < values.length; i ++) {
						currentValue += values[i].value;
					}

					var series = self.get('current').get('metricData').get('storagetrend');

					// If first value, initialize timeseries with current total across the board
					if (undefined === series) {
						var length = Math.abs(start);
						series = [];
						while (length--) {
							series[length] = currentValue;
						}
					}

					series.shift();
					series.push(currentValue);
					self.get('current').get('metricData').set('storagetrend', series.concat([]));

				});

			} else {

				var length = Math.abs(start);
				var series = [];
				while (length--) {
					series[length] = 0;
				}

				self.get('current').get('metricData').set('storagetrend', series.concat([]));

			}

			C.get('monitor', {
				method: 'getTimeSeries',
				params: [null, null, ['processed.count'], (C.__timeRange * -1), null, 'ACCOUNT_LEVEL']
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