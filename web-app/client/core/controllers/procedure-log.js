/*
 * Procedure Log Controller
 */

define(['core/controllers/runnable-log'], function (RunnableLogController) {

	var Controller = RunnableLogController.extend({

		init: function () {

			this.set('expectedPath', 'Procedure.Log');
			this.set('entityType', C.ENTITY_MAP['PROCEDURE']);

		}

	});

	Controller.reopenClass({
		type: 'ProcedureLog',
		kind: 'Controller'
	});

	return Controller;

});