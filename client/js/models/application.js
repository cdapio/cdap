//
// Application Model
//

define([], function () {
	return Em.Object.extend({
		href: function () {
			return '#/applications/' + this.get('name');
		}.property()
	});
});