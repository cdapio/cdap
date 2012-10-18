//
// Stream Controller
//

define([], function () {
	
	var attachedFlow = Em.Object.extend({

		init: function() {
			this._super();

			this.set('metricData', Em.Object.create());
			this.set('metricNames', {});

			this.set('id', this.get('flowId') || this.get('id') || this.get('meta').name);
			this.set('app', this.get('applicationId') || this.get('application'));

		},

		href: function () {
			return '#/flows/status/' + this.get('app') + ':' + this.get('id');
		}.property(),

		addMetricName: function (name) {

			this.get('metricNames')[name] = 1;

		},
		getUpdateRequest: function () {

			var metrics = [];
			for (var name in this.get('metricNames')) {
				metrics.push(name);
			}

			var app = '-';
			var id = '-';

			var accountId = 'demo';

			var start = C.__timeRange * -1;
			var self = this;

			var storageMetric = 'stream.storage.stream//' + accountId + '/' + this.get('id') + '.count';
			metrics.push(storageMetric);

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

				}


			}];

		}

	});

	return Em.ArrayProxy.create({
		types: Em.Object.create(),
		load: function (id) {

			var self = this;

			C.get('metadata', {
				method: 'getStream',
				params: ['Stream', {
					id: id
				}]
			}, function (error, response) {

				self.set('current', C.Mdl.Stream.create(response.params));
				C.interstitial.hide();
				
				self.set('types.StreamFlow', Em.ArrayProxy.create({content: []}));

				self.get('current').set('typeName', 'Stream');

				C.get('metadata', {
					method: 'getFlowsByStream',
					params: [id]
				}, function (error, response) {

					var flows = response.params;

					for (var i = 0; i < flows.length; i ++) {

						flows[i].streamId = id;

						self.get('types.StreamFlow').pushObject(attachedFlow.create(flows[i]));

					}

					self.startStats();

				});

			});

		},

		startStats: function () {
			var self = this;
			clearTimeout(this.updateTimeout);
			this.updateTimeout = setTimeout(function () {
				self.updateStats();
			}, 1000);
		},

		updateStats: function () {
			var self = this;

			if (!this.get('current')) {
				self.startStats();
				return;
			}

			// Update timeseries data for current flow.
			C.get.apply(C, this.get('current').getUpdateRequest());

			var flows = this.get('types.StreamFlow').content;
			for (var i = 0; i < flows.length; i ++) {
				C.get.apply(flows[i], flows[i].getUpdateRequest());
			}

			this.startStats();

		},

		unload: function () {

		}

	});

});