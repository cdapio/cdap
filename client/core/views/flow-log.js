define([
	], function () {
	
	return Em.View.extend({
		templateName: 'flow-log',
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

			C.router.transitionTo('flows.flow', {
				id: this.get('current').app + ':' + this.get('current').id
			});

		},

		goToHistory: function () {

			C.router.transitionTo('flows.history', {
				id: this.get('current').app + ':' + this.get('current').id
			});

		},


		loadRun: function (event) {

			$('.run-list').removeClass('run-list-active');

			var runId = $(event.target).parent().attr('id');
			this.get('controller').loadRun(runId);
			$(event.target).parent().addClass('run-list-active');

		}

	});
});