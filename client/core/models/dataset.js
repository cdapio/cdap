//
// DataSet Model
//

define([], function () {
	return Em.Object.extend({
		storage: '0B',
		typeName: function () {
			return [
				'Time Series'
			][this.get('type') || 0];
		}.property(),
		href: function () {
			return '#/data/' + this.get('id');
		}.property().cacheable()
	});
});