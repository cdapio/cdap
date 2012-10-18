//
// Query Model
//

define([], function () {
	return Em.Object.extend({
		metricData: null,
		metricNames: null,
		__loadingData: false,
		instances: 0,
		type: 'Query',
		plural: 'Query',
		isRunning: function () {

			return this.get('currentState') === 'RUNNING' ? true : false;

		}.property('currentState').cacheable(false),
		init: function() {
			this._super();

			this.set('metricData', Em.Object.create());
			this.set('metricNames', {});

			var app = this.get('application');
			var id = this.get('id');

			var self = this;

			C.get('manager', {
				method: 'status',
				params: [app, id, -1]
			}, function (error, response) {

				self.set('currentState', response.params.status);

			});

		},
		addMetricName: function (name) {

			this.get('metricNames')[name] = 1;

		},
		getUpdateRequest: function () {

			var self = this;

			var app = this.get('application'),
				id = this.get('id'),
				start = C.__timeRange * -1;

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
				params: [app, id, metrics, start, undefined, 'FLOW_LEVEL']
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

					metric = metric.replace(/\./g, '');
					self.get('metricData').set(metric, data);
					this.set('__loadingData', false);
				}

			}];

		},
		href: function () {
			return '#/flows/status/' + this.get('app') + ':' + this.get('id');
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

			if (this.currentState === 'RUNNING' ||
				this.currentState === 'PAUSING') {
				return 'btn-pause';
			} else {
				return 'btn-start';
			}

		}.property('currentState').cacheable(false),
		stopDisabled: function () {

			if (this.currentState === 'RUNNING') {
				return false;
			}
			return true;

		}.property('currentState'),
		startPauseDisabled: function () {

			if (this.currentState !== 'STOPPED' &&
				this.currentState !== 'PAUSED' &&
				this.currentState !== 'DEPLOYED' &&
				this.currentState !== 'RUNNING') {
				return true;
			}
			return false;

		}.property('currentState'),
		defaultAction: function () {
			return {
				'deployed': 'Start',
				'stopped': 'Start',
				'stopping': 'Start',
				'starting': 'Start',
				'running': 'Pause',
				'adjusting': '...',
				'draining': '...',
				'failed': 'Start'
			}[this.currentState.toLowerCase()];
		}.property('currentState')
	});
});
