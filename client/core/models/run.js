/*
 * Flow Run Model
 */

define([], function () {

	var Model = Em.Object.extend({

		elementId: function () {
			return 'run-' + this.get('rid');
		}.property(),
		started: function () {
			return this.startTime >= 0 ? $.timeago(this.startTime*1000) : 'Never';
		}.property('timeTrigger'),
		ended: function () {
			return this.endTime >= 0 ? $.timeago(this.endTime*1000) : 'Never';
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

	Model.reopenClass({
		type: 'Run',
		kind: 'Model'
	});

	return Model;

});