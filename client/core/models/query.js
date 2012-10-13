//
// Query Model
//

define([], function () {
	return Em.Object.extend({
		metricData: null,
		metricNames: null,
		__loadingData: false,
		instances: 0,
		type: 'Query',
		plural: 'Queries',
		init: function() {
			this._super();

			this.set('name', this.get('flowId'));

		}
	});
});