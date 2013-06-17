/*
 * Dashboard Controller
 */

define([], function () {

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),
		counts: Em.Object.create(),
		__remaining: -1,

		load: function () {

			/*
			 * This is decremented to know when loading is complete.
			 * There are 6 things to load. See __loaded below.
			 */
			this.__remaining = 5;

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

			var self = this;

			/*
			 * Load Apps
			 * Also load all Elements for each App to calculate per-App totals
			 */
			this.HTTP.getElements('App', function (objects) {
				var i = objects.length;
				while (i--) {
					objects[i] = C.App.create(objects[i]);

					self.HTTP.getElements('Stream', function (obj, arg) {
						objects[arg].set('counts.Stream', obj.length);
					}, objects[i].id, i);
					self.HTTP.getElements('Flow', function (obj, arg) {
						objects[arg].set('counts.Flow', obj.length);
					}, objects[i].id, i);
					self.HTTP.getElements('Batch', function (obj, arg) {
						objects[arg].set('counts.Batch', obj.length);
					}, objects[i].id, i);
					self.HTTP.getElements('Dataset', function (obj, arg) {
						objects[arg].set('counts.Dataset', obj.length);
					}, objects[i].id, i);
					self.HTTP.getElements('Procedure', function (obj, arg) {
						objects[arg].set('counts.Procedure', obj.length);
					}, objects[i].id, i);

				}
				self.get('elements.App').pushObjects(objects);
				self.get('counts').set('App', objects.length);

				self.__loaded();

			});

			/*
			 * Load all Streams to calculate counts and storage
			 */
			this.HTTP.getElements('Stream', function (objects) {

				self.get('elements.Stream').pushObjects(objects);
				self.get('counts').set('Stream', objects.length);
				self.__loaded();

			});

			/*
			 * Load all Flows to calculate counts
			 */
			this.HTTP.getElements('Flow', function (objects) {

				self.get('counts').set('Flow', objects.length);
				self.__loaded();

			});

			/*
			 * Load all Batches to calculate counts
			 */
			this.HTTP.getElements('Batch', function (objects) {

				self.get('counts').set('Batch', objects.length);
				self.__loaded();

			});

			/*
			 * Load all Datasets to calculate counts and storage
			 */
			this.HTTP.getElements('Dataset', function (objects) {

				self.get('elements.Dataset').pushObjects(objects);
				self.get('counts').set('Dataset', objects.length);
				self.__loaded();

			});

			/*
			 * Load all Procedures to calculate counts
			 */
			this.HTTP.getElements('Procedure', function (objects) {

				self.get('counts').set('Procedure', objects.length);
				self.__loaded();

			});

			/*
			 * Check disk space
			 */
			if (C.Env.cluster) {

				this.HTTP.get('/disk', function (disk) {
					if (disk) {
						var bytes = C.Util.bytes(disk.free);
						$('#diskspace').find('.sparkline-box-title').html(
							'Storage (' + bytes[0] + bytes[1] + ' Free)');
					}
				});

			}

		},

		__loaded: function () {

			if (!(--this.__remaining)) {

				var self = this;
				/*
				 * Give the chart Embeddables 100ms to configure
				 * themselves before updating.
				 */
				setTimeout(function () {
					self.updateStats();
				}, C.EMBEDDABLE_DELAY);

				this.interval = setInterval(function () {
					self.updateStats();
				}, C.POLLING_INTERVAL);

				/*
				 * Count hack to add Batch to Flows (Process)
				 */

				var batchCount = this.get('counts').get('Batch');
				var flowCount = this.get('counts').get('Flow');

				this.get('counts').set('Flow', batchCount + flowCount);

			}

		},

		unload: function () {

			clearInterval(this.interval);
			this.set('elements', Em.Object.create());
			this.set('counts', Em.Object.create());

		},

		updateStats: function () {

			var self = this;

			var streams = this.get('elements.Stream').content;
			var datasets = this.get('elements.Dataset').content;

			var accountId = C.Env.user.id;
			var metrics = [];
			var start = C.__timeRange * -1;

			/*
			 * Add Stream URIs to calculate storage usage by Streams
			 */
			for (var i = 0; i < streams.length; i ++) {
				metrics.push('stream.storage.stream//' + accountId + '/' + streams[i].id + '.count');
			}

			/*
			 * Add Dataset URIs to calculate storage usage by Datasets
			 */
			for (var i = 0; i < datasets.length; i ++) {
				metrics.push('dataset.storage.' + datasets[i].id + '.count');
			}

			if (metrics.length) {

				/*
				 * Get total disk usage and push to storage trend timeseries
				 * Someday we will have an API to give us a real timeseries
				 */
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

					// If first value, initialize timeseries with current total as straight line
					if (series === undefined) {
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

				/*
				 * No storage Elements, create a flat timeseries as default
				 */
				var length = Math.abs(start);
				var series = [];
				while (length--) {
					series[length] = 0;
				}

				self.get('model').get('metricData').set('storagetrend', series.concat([]));

			}

			/*
			 * Update processed.count for entire Reactor
			 */
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

			/*
			 * Update metrics for all Apps
			 */
			if (this.get('elements.App')) {

				var content = this.get('elements.App').get('content');

				for (var i = 0; i < content.length; i ++) {
					if (typeof content[i].getUpdateRequest === 'function') {
						C.get.apply(C, content[i].getUpdateRequest());
					}
				}

			}

		}

	});

	Controller.reopenClass({
		type: 'Index',
		kind: 'Controller'
	});

	return Controller;

});