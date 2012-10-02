//
// DataSet Model
//

define([], function () {
	return Em.Object.extend({
		href: function () {
			return '#/data/' + this.get('id');
		}.property().cacheable()
	});
});