define([
	], function () {
	
	return Em.View.extend({
		templateName: 'query-log',
		currentBinding: 'controller.current',

		setFlowletLabel: function (event) {

			var label = $(event.target).html();
			this.get('controller').set('__currentFlowletLabel', {
				'Arrival Rate': 'arrival.count',
				'Queue Depth': 'queue.depth',
				'Max Queue Depth': 'queue.maxdepth',
				'Processing Rate': 'processed.count'
			}[label]);

		},
		flowletLabelName: function () {

			return {
				'arrival.count': 'Arrival Rate',
				'queue.depth': 'Queue Depth',
				'queue.maxdepth': 'Max Queue Depth',
				'processed.count': 'Processing Rate'
			}[this.get('controller').__currentFlowletLabel];

		}.property('controller.__currentFlowletLabel'),

		goToStatus: function () {

			C.router.transitionTo('queries.query', {
				id: this.get('current').applicationId + ':' + this.get('current').id
			});

		},

	});
});