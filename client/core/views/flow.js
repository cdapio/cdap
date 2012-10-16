
define([
	], function () {
	
	return Em.View.extend({
		templateName: 'flow',
		currentBinding: 'controller.current',

		setFlowletLabel: function (event) {

			var label = $(event.target).html();
			this.get('controller').set('__currentFlowletLabel', {
				'Arrival Rate': 'arrival.count',
				'Queue Depth': 'queue.depth',
				'Max Queue Depth': 'queue.maxdepth',
				'Total Processed': 'processed.count'
			}[label]);

		},
		flowletLabelName: function () {

			return {
				'arrival.count': 'Arrival Rate',
				'queue.depth': 'Queue Depth',
				'queue.maxdepth': 'Max Queue Depth',
				'processed.count': 'Total Processed'
			}[this.get('controller').__currentFlowletLabel];

		}.property('controller.__currentFlowletLabel'),

		goToHistory: function () {

			C.router.transitionTo('flows.history', {
				id: this.get('current').app + ':' + this.get('current').id
			});

		},

		upload: function (event) {

			var view = C.Vw.Create.create({
				entityType: 'Flow'
			});
			view.append();

		},

		exec: function (event) {
			
			var control = $(event.target);
			if (event.target.tagName === "SPAN") {
				control = control.parent();
			}

			var id = control.attr('flow-id');
			var app = control.attr('flow-app');
			var action = control.attr('flow-action');

			if (action.toLowerCase() in C.Ctl.Flow) {
				C.Ctl.Flow[action.toLowerCase()](app, id, -1);
			}
		},
		promote: function () {

			var flow = this.current;
			
			C.interstitial.loading('Pushing to Cloud...', 'abc');

			C.get('far', {
				method: 'promote',
				params: [flow.applicationId, flow.id, flow.version]
			}, function (error, response) {
				
				C.interstitial.hide('abc');

			});

		},
		"delete": function () {
			
			if (C.Ctl.Flow.current.get('currentState') !== 'STOPPED' &&
				C.Ctl.Flow.current.get('currentState') !== 'DEPLOYED') {
				C.Vw.Informer.show('Cannot remove: Please stop the flow before removing.', 'alert-error');
			} else {
				C.Vw.Modal.show(
					"Delete Flow",
					"You are about to remove a Flow, which is irreversible. You can upload this flow again if you'd like. Do you want to proceed?",
					$.proxy(this.confirmed, this));
			}
		},
		confirmed: function (event) {

			var flow = this.current;
			
			C.get('far', {
				method: 'remove',
				params: [flow.applicationId, flow.id, flow.version]
			}, function (error, response) {

				if (error) {
					C.Vw.Informer.show(error.message, 'alert-error');
				} else {
					window.history.go(-1);
				}

			});
		}
	});

});