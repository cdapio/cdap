//
// Flow Model
//

define([], function () {
	return Em.Object.extend({
		ts: {}, // Used to store timeseries data, by type. (e.g. busyness)
		lastProcessed: 0,
		href: function () {
			if (this.get('applicationId')) {
				return '#/apps/' + this.get('applicationId') + '/' + this.get('flowId');
			} else {
				return '#/apps/' + this.get('meta').app + '/' + this.get('meta').name;
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
			return this.lastStarted >= 0 ? $.timeago(this.lastStarted) : 'Never';
		}.property('timeTrigger'),
		stopped: function () {
			return this.lastStopped >= 0 ? $.timeago(this.lastStopped) : 'Never';
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
