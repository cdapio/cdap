//
// Flow Model
// 
// WARNING: Property values cannot be an object (e.g. Em.Object.create()) or the object will be shared between instances.
//

define([], function () {
	return Em.Object.extend({
		metricData: null,
		metricNames: null,
		__loadingData: false,
		init: function() {
			this._super();

			this.set('metricData', Em.Object.create());
			this.set('metricNames', {});

			this.set('id', this.get('flowId') || this.get('meta').name);
			this.set('app', this.get('applicationId') || this.get('meta').app);

		},
		addMetricName: function (name) {

			this.get('metricNames')[name] = 1;
			// this.set('__loadingData', true);

		},
		getUpdateRequest: function () {

			var pointCount = 30;
			var self = this;

			var app = this.get('app'),
				id = this.get('id'),
				end = Math.round(new Date().getTime() / 1000),
				start = end - pointCount;

			var metrics = [];
			var metricNames = this.get('metricNames');
			for (var name in metricNames) {
				if (metricNames[name] === 1) {
					metrics.push(name);
				}
			}
			if (!metrics.length) {
				C.debug('Cannot update. Not tracking any metrics for ' + app + ':' + id);
				this.set('__loadingData', false);
				return;
			}

			return ['monitor', {
				method: 'getTimeSeries',
				params: [app, id, metrics, start, end, 'FLOW_LEVEL']
			}, function (error, response) {

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

					var a = 0;
					data = data.splice(0, 25);
					for (var k = data.length; k < 25; k++) {
						data.unshift(0);
						a ++;
					}

					metric = metric.replace(/\./g, '');
					self.get('metricData').set(metric, data);
					this.set('__loadingData', false);
				}

			}];

		},
		href: function () {
			if (this.get('applicationId')) {
				return '#/flows/' + this.get('applicationId') + ':' + this.get('flowId');
			} else {
				return '#/flows/' + this.get('meta').app + ':' + this.get('meta').name;
			}
		}.property(),
		undeployHref: function () {
			return this.get('href').replace('/flow/', '/undeploy/');
		}.property(),
		getMeta: function () {
			var arr = [];
			for (var m in this.meta) {
				arr.push({
					k: m,
					v: this.meta[m]
				});
			}
			return arr;
		}.property('meta'),
		started: function () {
			return this.lastStarted >= 0 ? $.timeago(this.lastStarted) : 'No Date';
		}.property('timeTrigger'),
		stopped: function () {
			return this.lastStopped >= 0 ? $.timeago(this.lastStopped) : 'No Date';
		}.property('timeTrigger'),
		actionIcon: function () {
			return {
				'deployed': 'btn-start',
				'stopped': 'btn-start',
				'running': 'btn-pause'
			}[this.currentState.toLowerCase()];
		}.property('currentState'),
		stopDisabled: function () {

			if (this.currentState.toLowerCase() === 'running') {
				return false;
			}
			return true;

		}.property('currentState'),
		statusClass: function () {
			return {
				'deployed': 'label label-info',
				'stopped': 'label',
				'stopping': 'label label-warning',
				'starting': 'label label-warning',
				'running': 'label label-success',
				'adjusting': 'label label-info',
				'draining': 'label label-info',
				'failed': 'label label-warning'
			}[this.currentState.toLowerCase()];
		}.property('currentState'),
		defaultActionClass: function () {
			return {
				'deployed': 'btn btn-info',
				'stopping': 'btn btn-warning',
				'starting': 'btn btn-warning',
				'stopped': 'btn btn-danger',
				'running': 'btn btn-success',
				'adjusting': 'btn btn-info',
				'draining': 'btn btn-info',
				'failed': 'btn btn-warning'
			}[this.currentState.toLowerCase()];
		}.property('currentState'),
		defaultAction: function () {
			return {
				'deployed': 'Start',
				'stopped': 'Start',
				'stopping': '...',
				'starting': '...',
				'running': 'Pause',
				'adjusting': '...',
				'draining': '...',
				'failed': 'Start'
			}[this.currentState.toLowerCase()];
		}.property('currentState')
	});
});
