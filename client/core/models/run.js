//
// Flow Run Model
//

define([], function () {
	return Em.Object.extend({
		href: function () {
			var flow = C.Ctl.Flow.get('current');
			if (flow) {
				return '#/flow/' + flow.get('meta').app + '/' + flow.get('meta').name + '/' + this.get('runId');
			}
		}.property(),
		elementId: function () {
			return 'run-' + this.get('rid');
		}.property(),
		started: function () {
			return this.startTime >= 0 ? $.timeago(this.startTime) : 'Never';
		}.property('timeTrigger'),
		ended: function () {
			return this.endTime >= 0 ? $.timeago(this.endTime) : 'Never';
		}.property('timeTrigger'),
		statusClass: function () {
			return {
				'stopped': 'label',
				'stopping': 'label label-warning',
				'running': 'label label-success',
				'failed': 'label label-warning'
			}[this.endStatus.toLowerCase()];
		}.property('endStatus'),
		detail: function () {
			var state = this.get('endStatus');
			return {
				'STOPPED': 'Stopped by user'
			}[state];
		}.property('endStatus')
	});
});