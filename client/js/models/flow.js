//
// Flow Model
//

define([], function () {
	return Em.Object.extend({
		href: function () {
			return '#/flows/' + this.get('id');
		}.property(),
		lastStarted: function () {
			return this.started ? $.timeago(this.started) : 'Never';
		}.property('timeTrigger'),
		lastStopped: function () {
			return this.stopped ? $.timeago(this.stopped) : 'Never';
		}.property('timeTrigger'),
		currentStatus: function () {
			return this.get('status').toUpperCase();
		}.property('status'),
		statusClass: function () {
			return {
				'stopped': 'label',
				'stopping': 'label label-warning',
				'running': 'label label-success',
				'error': 'label label-warning'
			}[this.status];
		}.property('status'),
		defaultActionClass: function () {
			return {
				'stopped': 'btn',
				'stopping': 'btn',
				'running': 'btn',
				'error': 'btn'
			}[this.status];
		}.property('status'),
		defaultAction: function () {
			return {
				'stopped': 'Start',
				'stopping': 'Wait',
				'running': 'Stop',
				'error': 'Detail'
			}[this.status];
		}.property('status')
	});
});