//
// Flow Model
//

define([], function () {
	return Em.Object.extend({
		href: function () {
			return '#/flow/' + this.get('applicationId') + '/' + this.get('flowId');
		}.property(),
		started: function () {
			return this.lastStarted >= 0 ? $.timeago(this.lastStarted) : 'Never';
		}.property('timeTrigger'),
		stopped: function () {
			return this.lastStopped >= 0 ? $.timeago(this.lastStopped) : 'Never';
		}.property('timeTrigger'),
		statusClass: function () {
			return {
				'deployed': 'label label-info',
				'stopped': 'label',
				'stopping': 'label label-warning',
				'running': 'label label-success',
				'failed': 'label label-warning'
			}[this.currentState.toLowerCase()];
		}.property('currentState'),
		defaultActionClass: function () {
			return {
				'deployed': 'btn btn-danger',
				'stopped': 'btn btn-danger',
				'stopping': 'btn',
				'running': 'btn btn-success',
				'failed': 'btn btn-warning'
			}[this.currentState.toLowerCase()];
		}.property('currentState'),
		defaultAction: function () {
			return {
				'deployed': 'Start',
				'stopped': 'Start',
				'stopping': 'Wait',
				'running': 'Stop',
				'failed': 'Start'
			}[this.currentState.toLowerCase()];
		}.property('currentState')
	});
});