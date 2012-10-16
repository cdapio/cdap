//
// Flowlet Model
//

define([], function () {
	return Em.Object.extend({
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
		getUpdateRequest: function () {

			var id = C.Ctl.Flow.current.id;
			var app = C.Ctl.Flow.current.app;

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

					/*
					data = data.splice(0, 25);
					for (var k = data.length; k < 25; k++) {
						data.unshift(0);
					}
					*/

					metric = metric.replace(/\./g, '');

					self.get('metricData').set(metric, data);
					this.set('__loadingData', false);

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
		}.property().cacheable(false),
		addInstances: function (value, done) {

			var instances = this.get('instances') + value;

			if (instances < 1 || instances > 64) {
				done('Cannot set instances. Please select an instance count > 1 and <= 64');
			} else {

				var current = this;
				var currentFlow = C.Ctl.Flow.get('current');

				var app = currentFlow.get('applicationId');
				var flow = currentFlow.get('id');
				var version = currentFlow.version;

				var flowlet = current.name;

				C.interstitial.loading('Setting instances for "' + flowlet + '" flowlet to ' + instances + '.', 'inst');
				$('#flowlet-container').hide();

				C.get('manager', {
					method: 'setInstances',
					params: [app, flow, version, flowlet, instances]
				}, function (error, response) {

					if (error) {
						C.Vw.Informer.show(error, 'alert-error');
					} else {
						current.set('instances', instances);
						C.Vw.Informer.show('Successfully set the instances for "' + flowlet + '" to ' + instances + '.', 'alert-success');
					}

					C.interstitial.hide('inst');

				});

			}
		}
	});
});