/*
 * Flowlet Model
 */

define([], function () {

	var Model = Em.Object.extend({
		metricData: null,
		metricNames: null,
		__loadingData: false,
		elementId: function () {

			return 'flowlet' + this.get('id');

		}.property().cacheable(),
		init: function() {
			this._super();

			this.set('metricData', Em.Object.create());
			this.set('metricNames', {});

			this.set('id', this.get('name'));

		},
		addMetricName: function (name) {

			this.get('metricNames')[name] = 1;

		},
		getUpdateRequest: function (flow) {

			var id = flow.name;
			var app = flow.app;

			var self = this;
			var pointCount = 30;

			var metrics = [];
			for (var name in this.get('metricNames')) {
				metrics.push(name);
			}

			var start = C.__timeRange * -1;

			return ['monitor', {
				method: 'getTimeSeries',
				params: [app, id, metrics, start, undefined, 'FLOWLET_LEVEL', this.get('id')]
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

					metric = metric.replace(/\./g, '');

					self.get('metricData').set(metric, data);
					self.set('__loadingData', false);

				}

			}];

		},
		label: 0,
		plural: function () {
			return this.instances === 1 ? '' : 's';
		}.property('instances'),
		doubleCount: function () {
			return 'Add ' + this.instances;
		}.property().cacheable(false),
		fitCount: function () {
			return 'No Change';
		}.property().cacheable(false)
	});

	Model.reopenClass({
		type: 'Flowlet',
		kind: 'Model',
		find: function (flowlet_id, http) {

			/*
			 * We will use this Flowlet and its ID to find the
			 * Flowlet model in the parent controller.
			 * See FlowletController.load()
			 */

			return C.Flowlet.create({
				'name': flowlet_id
			});

		}
	});

	return Model;

});