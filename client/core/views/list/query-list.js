
define([
	'lib/text!../../partials/list/query-list.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template),
		didInsertElement: function () {



		},
		start: function (event) {

			var id = $(event.target).attr('flowId');
			var app = $(event.target).attr('applicationId');

			$(event.target).parent().html('<img src="/assets/img/chart-loading.gif" />');

			C.socket.request('manager', {
				method: 'start',
				params: [app, id, -1, 'QUERY']
			}, function (error, response) {

				window.location.reload();

			});

		},
		stop: function (event) {

			var id = $(event.target).attr('flowId');
			var app = $(event.target).attr('applicationId');

			$(event.target).parent().html('<img src="/assets/img/chart-loading.gif" />');

			C.socket.request('manager', {
				method: 'stop',
				params: [app, id, -1, 'QUERY']
			}, function (error, response) {

				setTimeout(function () {
					window.location.reload();
				}, 1000);

			});

		}

	});

});