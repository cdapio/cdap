/*
 * Flow Model
 */

define([], function () {

	var Model = Em.Object.extend({

		href: function () {
			return '#/flows/' + this.get('id');
		}.property('id'),
		metricData: null,
		metrics: null,
		__loadingData: false,
		instances: 0,
		version: -1,
		currentState: '',
		type: 'Flow',
		plural: 'Flows',
		init: function() {
			this._super();

			this.set('timeseries', Em.Object.create());
			this.set('metrics', []);

			this.set('name', (this.get('flowId') || this.get('id') || this.get('meta').name));

			this.set('app', this.get('applicationId') || this.get('application') || this.get('meta').app);
			this.set('id', this.get('app') + ':' +
				(this.get('flowId') || this.get('id') || this.get('meta').name));

		},
		addMetricName: function (name) {

			name = name.replace(/{parent}/, this.get('app'));
			name = name.replace(/{id}/, this.get('name'));

			this.get('metrics').push(name);

			return name;

		},
		update: function (http) {

			var self = this;

			var app_id = this.get('app'),
				flow_id = this.get('name');

			http.rpc('runnable', 'status', [app_id, flow_id, -1],
				function (response) {

					if (response.result) {
						self.set('currentState', response.result.status);
					}

			});

			return this.get('metrics').slice(0);

			/*
			return ['monitor', {
				method: 'getTimeSeries',
				params: [app_id, flow_id, metrics, start, undefined, 'FLOW_LEVEL']
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
					self.set('__loadingData', false);
				}

			}];
			*/

		},
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
		isRunning: function() {

			if (this.currentState !== 'RUNNING') {
				return false;
			}
			return true;

		}.property('currentState'),
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
		startDisabled: function () {
			if (this.currentState === 'RUNNING') {
				return true;
			}
			return false;
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

	Model.reopenClass({
		type: 'Flow',
		kind: 'Model',
		find: function(model_id, http) {

			var promise = Ember.Deferred.create();

			var model_id = model_id.split(':');
			var app_id = model_id[0];
			var flow_id = model_id[1];

			http.rest('apps', app_id, 'flows', flow_id, function (model, error) {

				model.applicationId = app_id;
				model = C.Flow.create(model);

				http.rpc('runnable', 'status', [app_id, flow_id, -1],
					function (response) {

						if (response.error) {
							promise.reject(response.error);
						} else {
							model.set('currentState', response.result.status);
							promise.resolve(model);
						}

				});

			});

			return promise;

		}

	});

	return Model;

});
