//
// Flow Model
//

define([], function () {
	return Em.Object.extend({
		href: function () {
			return '#/flow/' + this.get('applicationId') + '/' + this.get('flowId');
		}.property(),
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
				'undefined': 'btn',
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
				'undefined': 'Start',
				'running': 'Stop',
				'failed': 'Start'
			}[this.currentState.toLowerCase()];
		}.property('currentState')
	});
});