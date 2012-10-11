//
// DataSet Status View
//

define([
	], function () {
	
	return Em.View.extend({
		templateName: 'dataset',
		currentBinding: 'controller.current',
		"delete": function () {
			
			C.Vw.Modal.show(
				"Delete Dataset",
				"Are you sure you would like to delete this Dataset? This action is not reversible.",
				$.proxy(this.confirmed, this));

		},
		confirmed: function (event) {

			var flow = this.current;
			
			C.get('metadata', {
				method: 'deleteDataset',
				params: ['Dataset', {
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