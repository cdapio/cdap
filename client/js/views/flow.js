
define([
	'lib/text!../../templates/flow.html'
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template),
		currentBinding: 'App.Controllers.Flow.current',
		runBinding: 'App.Controllers.Flow.run',
		historyBinding: 'App.Controllers.Flow.history',
		exec: function (event) {

			var control = $(event.target);
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

			App.socket.request('manager', {
				method: 'promote',
				params: [flow.meta.app, flow.meta.name, flow.version]
			}, function (error, response) {

				if (error) {
					App.informer.show(error.message, 'alert-error');
				} else {
					App.informer.show('Successfully pushed to cloud. Navigate here to view: {{cloud-site-url}}');
				}

			});

		},
		"delete": function () {
			
			if (App.Controllers.Flow.current.get('currentState') !== 'STOPPED' &&
				App.Controllers.Flow.current.get('currentState') !== 'DEPLOYED') {
				App.informer.show('Cannot remove: Please stop the flow before removing.', 'alert-error');
			} else {
				$('#modal-from-dom').modal('show');
			}
		},
		confirmed: function (event) {

			var control = $(event.target);
			var id = control.attr('flow-id');
			var app = control.attr('flow-app');

			App.socket.request('manager', {
				method: 'remove',
				params: ['demo', app, id]
			}, function (error, response) {

				$('#modal-from-dom').modal('hide');

				if (error) {
					App.informer.show(error.message, 'alert-error');
				} else {
					App.router.set('location', '#/');
				}

			});
		},
		canceled: function () {
			$('#modal-from-dom').modal('hide');
		}
	});

});