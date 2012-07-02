
define([
	'lib/text!../../templates/flow.html'
	], function (Template) {
	
	return Em.View.create({
		template: Em.Handlebars.compile(Template),
		currentBinding: 'App.Controllers.Flow.current',
		runBinding: 'App.Controllers.Flow.run',
		historyBinding: 'App.Controllers.Flow.history',
		exec: function (event) {

			var control = $(event.srcElement);
			var id = control.attr('flow-id');
			var app = control.attr('flow-app');
			var action = control.attr('flow-action');

			if (action.toLowerCase() in App.Controllers.Flows) {
				App.Controllers.Flows[action.toLowerCase()](app, id, -1);
			}
		},
		loadRun: function (event) {

			var td = $(event.srcElement);
			var href = td.parent().attr('href');
			
			App.router.set('location', href);
		},
		showError: function (message) {
			$('#flow-alert').removeClass('alert-success')
				.addClass('alert-error').html('Error: ' + message).show();
		},
		"delete": function () {
			
			if (App.Controllers.Flow.current.get('currentState') !== 'STOPPED' &&
				App.Controllers.Flow.current.get('currentState') !== 'DEPLOYED') {
				App.Views.Flow.showError('Cannot remove: Please stop the flow before removing.');
			} else {
				$('#modal-from-dom').modal('show');
			}
		},
		confirmed: function (event) {

			var control = $(event.srcElement);
			var id = control.attr('flow-id');
			var app = control.attr('flow-app');

			App.socket.request('manager', {
				method: 'remove',
				params: ['demo', app, id]
			}, function (error, response) {

				$('#modal-from-dom').modal('hide');

				if (error) {
					App.Views.Flow.showError(error.message);
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