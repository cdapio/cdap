//
// App Status View
//

define([
	], function () {
	
	return Em.View.extend({
		templateName: 'application',
		currentBinding: 'controller.current',
		"delete": function () {
			
			C.Vw.Modal.show(
				"Delete Application",
				"Are you sure you would like to delete this Application? This action is not reversible.",
				$.proxy(this.confirmed, this));

		},
		confirmed: function (event) {

			var flow = this.current;
			
			C.get('metadata', {
				method: 'deleteApplication',
				params: ['Application', {
					id: this.current.id
				}]
			}, function (error, response) {

				if (error) {
					C.Vw.Informer.show(error.message, 'alert-error');
				} else {
					window.history.go(-1);
				}

			});
		},
		startAllFlows: function (event) {

			var container = $(event.target).parent();

			container.find('button').hide();
			container.find('img').show();

			C.Ctl.Application.startAllFlows(function () {
				container.find('button').show();
				container.find('img').hide();
			});

		},
		stopAllFlows: function (event) {

			var container = $(event.target).parent();
			
			container.find('button').hide();
			container.find('img').show();

			C.Ctl.Application.stopAllFlows(function () {
				container.find('button').show();
				container.find('img').hide();
			});

		},
		startAllQueries: function (event) {

			var container = $(event.target).parent();

			container.find('button').hide();
			container.find('img').show();

			C.Ctl.Application.startAllQueries(function () {
				container.find('button').show();
				container.find('img').hide();
			});

		},
		stopAllQueries: function (event) {

			var container = $(event.target).parent();

			container.find('button').hide();
			container.find('img').show();

			C.Ctl.Application.stopAllQueries(function () {
				container.find('button').show();
				container.find('img').hide();
			});

		}
	});
});