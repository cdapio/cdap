
define([
	'lib/text!../../partials/list/query-list.html'
	], function (Template) {
	
	return Em.View.extend({
		template: Em.Handlebars.compile(Template) /*,
		start: function (event) {

			var id = $(event.target).attr('flowId');
			var app = $(event.target).attr('applicationId');

			C.socket.request('manager', {
				method: 'start',
				params: [app, id, -1]
			}, function (error, response) {

				window.alert('Started');

			});

		}
		*/

	});

});