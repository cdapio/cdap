//
// Flow Model
//

define([], function () {
	return Em.Object.extend({
		href: function () {
			if (this.get('applicationId')) {
				return '#/flow/' + this.get('applicationId') + '/' + this.get('flowId');
			} else {
				return '#/flow/' + this.get('meta').app + '/' + this.get('meta').name;
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
			return this.lastStarted >= 0 ? $.timeago(this.lastStarted * 1000) : 'Never';
		}.property('timeTrigger'),
		stopped: function () {
			return this.lastStopped >= 0 ? $.timeago(this.lastStopped * 1000) : 'Never';
		}.property('timeTrigger'),
		statusClass: function () {
			return {
				'deployed': 'label label-info',
				'stopped': 'label',
				'stopping': 'label label-warning',
				'starting': 'label label-warning',
				'running': 'label label-success',
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
				'failed': 'btn btn-warning'
			}[this.currentState.toLowerCase()];
		}.property('currentState'),
		defaultAction: function () {
			return {
				'deployed': 'Start',
				'stopped': 'Start',
				'stopping': '...',
				'starting': '...',
				'running': 'Stop',
				'failed': 'Start'
			}[this.currentState.toLowerCase()];
		}.property('currentState')
	});
});
