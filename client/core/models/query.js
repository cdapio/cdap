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

			this.set('metricData', Em.Object.create());
			this.set('metricNames', {});

			this.set('id', this.get('flowId') || this.get('meta').name);
			this.set('app', this.get('applicationId') || this.get('meta').app);
		}
	});
});