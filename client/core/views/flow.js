
define([
	'lib/text!../partials/flow.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		currentBinding: 'App.Controllers.Flow.current',
		runBinding: 'App.Controllers.Flow.run',
		historyBinding: 'App.Controllers.Flow.history',
		exec: function (event) {
			
			var control = $(event.target);
			if (event.target.tagName === "SPAN") {
				control = control.parent();
			}

			var id = control.attr('flow-id');
			var app = control.attr('flow-app');
			var action = control.attr('flow-action');

			if (action.toLowerCase() in App.Controllers.Flows) {
				App.Controllers.Flows[action.toLowerCase()](app, id, -1);
			}
		},
		loadRun: function (event) {

			var td = $(event.target);
			var href = td.parent().attr('href');
			
			App.router.set('location', href);
		},
		promote: function () {

			var flow = this.current;
			
			App.interstitial.loading('Pushing to Cloud...');

			App.socket.request('far', {
				method: 'promote',
				params: [flow.meta.app, flow.meta.name, flow.version]
			}, function (error, response) {

				if (error) {
					App.Views.Informer.show(error, 'alert-error');
				} else {
					App.Views.Informer.show('Successfully pushed to cloud.');
				}
				App.interstitial.hide();

			});

		},
		"delete": function () {
			
			if (App.Controllers.Flow.current.get('currentState') !== 'STOPPED' &&
				App.Controllers.Flow.current.get('currentState') !== 'DEPLOYED') {
				App.Views.Informer.show('Cannot remove: Please stop the flow before removing.', 'alert-error');
			} else {
				App.Views.Modal.show(
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
					App.Views.Informer.show(error.message, 'alert-error');
				} else {
					App.router.transitionTo('home');
				}

			});
		}
	});

});