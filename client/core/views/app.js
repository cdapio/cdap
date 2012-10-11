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
				"Delete Stream",
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
					C.router.transitionTo('home');
				}

			});
		}
	});
});