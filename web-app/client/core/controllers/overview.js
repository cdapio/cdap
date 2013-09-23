/*
 * Overview Controller
 */

define([], function () {

	var DASH_CHART_COUNT = 60;

	var Controller = Em.Controller.extend({

		elements: Em.Object.create(),
		counts: Em.Object.create(),
		__remaining: -1,

		aggregates: Em.Object.create(),
		timeseries: Em.Object.create(),
		value: Em.Object.create(),

		load: function () {

			this.set('elements.App', Em.ArrayProxy.create({content: []}));
			this.clearTriggers(true);

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
			this.HTTP.rest('apps', function (objects) {
				var i = objects.length;

				while (i--) {
					objects[i] = C.App.create(objects[i]);

					(function (id, index) {

						self.HTTP.rest('apps', id, 'streams', function (items) {
							objects[index].set('counts.Stream', items.length);
						});

						self.HTTP.rest('apps', id, 'flows', function (items) {
							objects[index].set('counts.Flow', items.length);
						});

						self.HTTP.rest('apps', id, 'datasets', function (items) {
							objects[index].set('counts.Dataset', items.length);
						});

						self.HTTP.rest('apps', id, 'procedures', function (items) {
							objects[index].set('counts.Procedure', items.length);
						});

					})(objects[i].id, i);

				}
				self.get('elements.App').pushObjects(objects);
				self.get('counts').set('App', objects.length);

				/*
				 * Give the chart Embeddables 100ms to configure
				 * themselves before updating.
				 */
				setTimeout(function () {
					self.updateStats();
				}, C.EMBEDDABLE_DELAY);

				self.interval = setInterval(function () {
					self.updateStats();
				}, C.POLLING_INTERVAL);

			});

			/*
			 * Check disk space
			 */
			if (C.Env.cluster) {

				this.HTTP.get('disk', function (disk) {
					if (disk) {
						var bytes = C.Util.bytes(disk.free);
						$('#diskspace').find('.sparkline-box-title').html(
							'Storage (' + bytes[0] + bytes[1] + ' Free)');
					}
				});

			}

		},

		unload: function () {

			clearInterval(this.interval);
			this.set('elements', Em.Object.create());
			this.set('counts', Em.Object.create());

			this.set('aggregates', Em.Object.create());
			this.set('timeseries', Em.Object.create());

		},

		ajaxCompleted: function () {
			return this.get('timeseriesCompleted') && this.get('aggregatesCompleted') && this.get('miscCompleted');
		},

		clearTriggers: function (value) {
			this.set('timeseriesCompleted', value);
			this.set('aggregatesCompleted', value);
			this.set('miscCompleted', value);
		},

		updateStats: function () {

			if (!this.ajaxCompleted() || C.currentPath !== 'Overview') {
				return;
			}

			var models = this.get('elements.App').get('content');

			var now = new Date().getTime();

			// Add a two second buffer to make sure we have a full response.
			var start = now - ((C.__timeRange + 2) * 1000);
			start = Math.floor(start / 1000);

			this.clearTriggers(false);

			// Scans models for timeseries metrics and updates them.
			C.Util.updateTimeSeries(models, this.HTTP, this);

			// Scans models for aggregate metrics and udpates them.
			C.Util.updateAggregates(models, this.HTTP, this);

			// Hax. Count is timerange because server treats end = start + count (no downsample yet)
			var queries = [
				'/reactor/collect.events?count=' + C.__timeRange + '&start=' + start,
				'/reactor/process.busyness?count=' + C.__timeRange + '&start=' + start,
				'/reactor/store.bytes?count=' + C.__timeRange + '&start=' + start,
				'/reactor/query.requests?count=' + C.__timeRange + '&start=' + start
			], self = this;

			function lastValue(arr) {
				return arr[arr.length - 1].value;
			}

			this.HTTP.post('metrics', queries, function (response) {
				self.set('miscCompleted', true);
				if (response.result) {

					var result = response.result;

					self.set('timeseries.collect', result[0].result.data);
					self.set('timeseries.process', result[1].result.data);
					self.set('timeseries.store', result[2].result.data);
					self.set('timeseries.query', result[3].result.data);

					self.set('value.collect', lastValue(result[0].result.data));
					self.set('value.query', lastValue(result[3].result.data));

					self.set('value.process', lastValue(result[1].result.data));

					var store = C.Util.bytes(lastValue(result[2].result.data));
					self.set('value.store', {
						label: store[0],
						unit: store[1]
					});

				}

			});

		}

	});

	Controller.reopenClass({
		type: 'Overview',
		kind: 'Controller'
	});

	return Controller;

});