/*
 * Flow Run Model
 */

define([], function () {

	var Model = Em.Object.extend({

		elementId: function () {
			return 'run-' + this.get('rid');
		}.property(),

		started: function () {
			return this.start >= 0 ? $.timeago(this.start*1000) : 'Never';
		}.property('timeTrigger'),

		startDate: function () {
			return new Date(this.start*1000);
		}.property('start'),

		ended: function () {
			return this.end >= 0 ? $.timeago(this.end*1000) : 'Never';
		}.property('timeTrigger'),

		endDate: function () {
			return new Date(this.end*1000);
		}.property('end'),

		statusClass: function () {
			return {
				'stopped': 'label',
				'stopping': 'label label-warning',
				'running': 'label label-success',
				'failed': 'label label-warning'
			}[this.status.toLowerCase()];
		}.property('status'),

		detail: function () {
			var state = this.get('status');
			return {
				'STOPPED': 'Completed',
				'ERROR': 'Failed'
			}[state];
		}.property('status')

	});

	Model.reopenClass({
		type: 'Run',
		kind: 'Model'
	});

	return Model;

});