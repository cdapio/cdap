/*
 * Procedure Log Controller
 */

define(['core/controllers/runnable-log'], function (RunnableLogController) {

	var Controller = RunnableLogController.extend({

		init: function () {

			this.set('expectedPath', 'Procedure.Log');

		}

	});

	Controller.reopenClass({
		type: 'ProcedureLog',
		kind: 'Controller'
	});

	return Controller;

});