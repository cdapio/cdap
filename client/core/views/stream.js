//
// Stream Status View
//

define([
	], function () {
	
	return Em.View.extend({
		templateName: 'stream',
		currentBinding: 'controller.current',
		"delete": function () {
			
			C.Vw.Modal.show(
				"Delete Stream",
				"Are you sure you would like to delete this Stream? This action is not reversible.",
				$.proxy(this.confirmed, this));

		},
		confirmed: function (event) {

			var flow = this.current;
			
			C.get('metadata', {
				method: 'deleteStream',
				params: ['Stream', {
					id: this.current.id
				}]
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