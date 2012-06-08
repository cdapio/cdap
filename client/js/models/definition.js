//
// Flow Definition Model
//

define([], function () {
	return Em.Object.extend({
		href: function () {
			return '#/flow/' + this.get('meta').app + '/' + this.get('meta').name;
		}.property(),
		currentStatus: 'UNKNOWN',
		defaultActionClass: 'btn',
		defaultAction: '...'
	});
});