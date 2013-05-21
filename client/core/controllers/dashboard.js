//
// Dashboard Controller
//

define([], function () {

	var Controller = Em.Controller.extend({
		elements: Em.Object.create(),
		counts: Em.Object.create(),
		__toLoad: 5,
		load: function () {

			var self = this;

			this.__toLoad = 5;

			this.set('elements.App', Em.ArrayProxy.create({content: []}));
			this.set('elements.Stream', Em.ArrayProxy.create({content: []}));
			this.set('elements.Dataset', Em.ArrayProxy.create({content: []}));

			this.set('model', Em.Object.create({
				addMetricName: function (metric) {
					this.get('metricNames')[metric] = 1;
				},
				metricNames: {},
				metricData: Em.Object.create()
			}));

			function finish () {
				/*
				* Give the chart Embeddables 100ms to configure
				* themselves before updating.
				*/
				setTimeout(function () {
					self.getStats();
				}, 100);
			}

			C.Api.getElements('App', function (objects) {
				var i = objects.length;
				while (i--) {
					objects[i] = C.App.create(objects[i]);

					C.Api.getElements('Stream', function (obj, arg) {
						objects[arg].set('counts.Stream', obj.length);
					}, objects[i].id, i);
					C.Api.getElements('Flow', function (obj, arg) {
						objects[arg].set('counts.Flow', obj.length);
					}, objects[i].id, i);
					C.Api.getElements('Dataset', function (obj, arg) {
						objects[arg].set('counts.Dataset', obj.length);
					}, objects[i].id, i);
					C.Api.getElements('Procedure', function (obj, arg) {
						objects[arg].set('counts.Procedure', obj.length);
					}, objects[i].id, i);

				}
				self.get('elements.App').pushObjects(objects);
				self.get('counts').set('App', objects.length);

				self.__toLoad--;
				if (!self.__toLoad) {
					finish();
					self.__toLoad = 5;
				}

			});

			C.Api.getElements('Stream', function (objects) {

				// For use by storage trend
				self.get('elements.Stream').pushObjects(objects);

				// For use by account stream count
				self.get('counts').set('Stream', objects.length);

				self.__toLoad--;
				if (!self.__toLoad) {
					finish();
				}

			});
			C.Api.getElements('Flow', function (objects) {
				self.get('counts').set('Flow', objects.length);

				self.__toLoad--;
				if (!self.__toLoad) {
					finish();
				}
			});
			C.Api.getElements('Dataset', function (objects) {

				// For use by storage trend
				self.get('elements.Dataset').pushObjects(objects);

				// For use by account datset count
				self.get('counts').set('Dataset', objects.length);

				self.__toLoad--;
				if (!self.__toLoad) {
					finish();
				}
			});
			C.Api.getElements('Procedure', function (objects) {
				self.get('counts').set('Procedure', objects.length);

				self.__toLoad--;
				if (!self.__toLoad) {
					finish();
				}
			});

			/**
			 * Check disk space.
			 */
			if (C.Env.cluster) {

				$.getJSON('/disk', function (status) {

					var bytes = C.Util.bytes(status.free);
					$('#diskspace').find('.sparkline-box-title').html(
						'Storage (' + bytes[0] + bytes[1] + ' Free)');

				});

			}

		},
		__timeout: null,
		getStats: function () {

			var self = this, objects, content;

			var streams = this.get('elements.Stream').content;
			var datasets = this.get('elements.Dataset').content;

			var accountId = C.Env.user.id;
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

					var series = self.get('model').get('metricData').get('storagetrend');

					// If first value, initialize timeseries with current total across the board
					if (undefined === series) {
						var length = Math.abs(start) - 21;
						series = [];
						while (length--) {
							series[length] = currentValue;
						}
					}

					series.shift();
					series.push(currentValue);
					self.get('model').get('metricData').set('storagetrend', series.concat([]));

				});

			} else {

				var length = Math.abs(start) - 21;
				var series = [];
				while (length--) {
					series[length] = 0;
				}

				self.get('model').get('metricData').set('storagetrend', series.concat([]));

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
					self.get('model').get('metricData').set(metric, data);

				}


			});

			if ((objects = this.get('elements.App'))) {

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
			this.set('elements', Em.Object.create());
		}
	});

	Controller.reopenClass({
		type: 'Index',
		kind: 'Controller'
	});

	return Controller;

});