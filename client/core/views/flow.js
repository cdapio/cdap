
define([
	'lib/text!../partials/flow.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		currentBinding: 'controller.current',
		historyBinding: 'controller.history',
		exec: function (event) {
			
			var control = $(event.target);
			if (event.target.tagName === "SPAN") {
				control = control.parent();
			}

			var id = control.attr('flow-id');
			var app = control.attr('flow-app');
			var action = control.attr('flow-action');

			if (action.toLowerCase() in C.Ctl.Flows) {
				C.Ctl.Flows[action.toLowerCase()](app, id, -1);
			}
		},
		promote: function () {

			var flow = this.current;
			
			App.interstitial.loading('Pushing to Cloud...');

			App.socket.request('far', {
				method: 'promote',
				params: [flow.meta.app, flow.meta.name, flow.version]
			}, function (error, response) {

				if (error) {
					C.Vw.Informer.show(error, 'alert-error');
				} else {
					C.Vw.Informer.show('Successfully pushed to cloud.');
				}
				App.interstitial.hide();

			});

		},
		"delete": function () {
			
			if (C.Ctl.Flow.current.get('currentState') !== 'STOPPED' &&
				C.Ctl.Flow.current.get('currentState') !== 'DEPLOYED') {
				C.Vw.Informer.show('Cannot remove: Please stop the flow before removing.', 'alert-error');
			} else {
				C.Vw.Modal.show(
					"Delete Flow",
					"You are about to remove a flow, which is irreversible. You can upload this flow again if you'd like. Do you want to proceed?",
					$.proxy(this.confirmed, this));
			}
		},
		confirmed: function (event) {

			var flow = this.current;
			
			App.socket.request('far', {
				method: 'remove',
				params: [flow.meta.app, flow.meta.name, flow.version]
			}, function (error, response) {

				if (error) {
					C.Vw.Informer.show(error.message, 'alert-error');
				} else {
					App.router.transitionTo('home');
				}

			});
		}
	});

});