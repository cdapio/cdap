
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
		delete: function () {
			var id = 0;
			$('#modal-from-dom').data('id', id).modal('show');
		},
		confirmed: function (event) {

			var control = $(event.srcElement);
			var id = control.attr('flow-id');
			var app = control.attr('flow-app');

			App.socket.request('manager', {
				method: 'remove',
				params: ['demo', app, id]
			}, function (response) {
				$('#modal-from-dom').modal('hide');
				App.router.set('location', '#/');
			});
		},
		canceled: function () {
			$('#modal-from-dom').modal('hide');
		}
	});

});