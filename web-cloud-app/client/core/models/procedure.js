/*
 * Procedure Model
 */

define([], function () {

	var Model = Em.Object.extend({
		type: 'Procedure',
		plural: 'Procedures',
		href: function () {
			return '#/procedures/' + this.get('id');
		}.property('id'),

		instances: 0,
		version: -1,
		currentState: '',

		init: function() {
			this._super();

			this.set('timeseries', Em.Object.create());
			this.set('metrics', []);

			this.set('name', (this.get('flowId') || this.get('id') || this.get('meta').name));

			this.set('app', this.get('applicationId') || this.get('application'));
			this.set('id', this.get('app') + ':' +
				(this.get('flowId') || this.get('id') || this.get('meta').name));

		},
		controlLabel: function () {

			if (this.get('isRunning')) {
				return 'Stop';
			} else {
				return 'Start';
			}

		}.property('currentState').cacheable(false),

		interpolate: function (path) {

			return path.replace(/\{parent\}/, this.get('app'))
				.replace(/\{id\}/, this.get('name'));

		},

		trackMetric: function (path, kind, label) {

			this.get(kind).set(path = this.interpolate(path), label || []);
			return path;

		},

		updateState: function (http) {

			var self = this;

			var app_id = this.get('app'),
				procedure_id = this.get('name');

			http.rpc('runnable', 'status', [app_id, procedure_id, -1, 'QUERY'],
				function (response) {

					if (response.result) {
						self.set('currentState', response.result.status);
					}

			});

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
		isRunning: function () {

			return this.get('currentState') === 'RUNNING' ? true : false;

		}.property('currentState').cacheable(false),
		started: function () {
			return this.lastStarted >= 0 ? $.timeago(this.lastStarted) : 'No Date';
		}.property('timeTrigger'),
		stopped: function () {
			return this.lastStopped >= 0 ? $.timeago(this.lastStopped) : 'No Date';
		}.property('timeTrigger'),
		actionIcon: function () {

			if (this.currentState === 'RUNNING' ||
				this.currentState === 'PAUSING') {
				return 'btn-stop';
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
			if (!this.currentState) {
				return '...';
			}
			return {
				'deployed': 'Start',
				'stopped': 'Start',
				'stopping': 'Start',
				'starting': 'Start',
				'running': 'Stop',
				'adjusting': '...',
				'draining': '...',
				'failed': 'Start'
			}[this.currentState.toLowerCase()];
		}.property('currentState')
	});

	Model.reopenClass({
		type: 'Procedure',
		kind: 'Model',
		find: function(model_id, http) {

			var promise = Ember.Deferred.create();

			var model_id = model_id.split(':');
			var app_id = model_id[0];
			var procedure_id = model_id[1];

			http.rest('apps', app_id, 'procedures', procedure_id, function (model, error) {

				model.applicationId = app_id;
				model = C.Procedure.create(model);

				http.rpc('runnable', 'status', [app_id, procedure_id, -1, 'QUERY'],
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
