/*
 * Batch Log Controller
 */

define(['core/controllers/runnable-log'], function (RunnableLogController) {

	var Controller = RunnableLogController.extend({

		init: function () {

			this.set('expectedPath', 'Batch.Log');

		}

	});

	Controller.reopenClass({
		type: 'BatchLog',
		kind: 'Controller'
	});

	return Controller;

});